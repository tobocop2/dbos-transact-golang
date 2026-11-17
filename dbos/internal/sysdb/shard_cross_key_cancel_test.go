package sysdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// shardedTestSchema builds a minimal sharded layout for the two tables a cross-key child
// start and a cancel touch: workflow_status and operation_outputs keyed by shard_key.
// Core links no sharding backend, so this stands in for the commercial migrations.
func shardedTestSchema(t *testing.T, ctx context.Context, schema string) *pgxpool.Pool {
	t.Helper()
	if os.Getenv("DBOS_TEST_BACKEND") == "sqlite" {
		t.Skip("a sharded system database runs on Postgres only")
	}
	databaseURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseURL == "" {
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", url.QueryEscape(password))
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
	require.NoError(t, err)
	t.Cleanup(func() { pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema)) }) //nolint:errcheck
	require.NoError(t, RunMigrations(ctx, pool, schema, false, slog.New(slog.DiscardHandler)))
	for _, stmt := range []string{
		`ALTER TABLE %[1]q.workflow_status DROP CONSTRAINT workflow_status_pkey CASCADE`,
		`ALTER TABLE %[1]q.workflow_status ADD COLUMN shard_key TEXT NOT NULL DEFAULT '', ADD COLUMN parent_shard_key TEXT`,
		`ALTER TABLE %[1]q.workflow_status ADD PRIMARY KEY (shard_key, workflow_uuid)`,
		`ALTER TABLE %[1]q.operation_outputs DROP CONSTRAINT operation_outputs_pkey`,
		`ALTER TABLE %[1]q.operation_outputs ADD COLUMN shard_key TEXT NOT NULL DEFAULT '', ADD COLUMN child_shard_key TEXT`,
		`ALTER TABLE %[1]q.operation_outputs ADD PRIMARY KEY (shard_key, workflow_uuid, function_id)`,
		`DO $$ DECLARE c text; BEGIN
			SELECT conname INTO c FROM pg_constraint WHERE conrelid = '%[1]s.workflow_input'::regclass AND contype = 'p';
			EXECUTE format('ALTER TABLE %%I.workflow_input DROP CONSTRAINT %%I', '%[1]s', c);
		END $$`,
		`ALTER TABLE %[1]q.workflow_input ADD COLUMN shard_key TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE %[1]q.workflow_input ADD PRIMARY KEY (shard_key, workflow_uuid)`,
	} {
		_, err := pool.Exec(ctx, fmt.Sprintf(stmt, schema))
		require.NoError(t, err, stmt)
	}
	return pool
}

// startCrossKeyChild makes the same system database calls, in the same order, as the
// start of a child under another shard key (insertCrossKeyChild in dbos/workflow.go).
// held runs between the parent's owner check and the child's insert.
func startCrossKeyChild(ctx context.Context, s *SysDB, parent, parentKey, owner, child, childKey string, held func()) error {
	if err := s.CheckOwner(ctx, parent, owner); err != nil {
		return err
	}
	held()
	tx, err := s.Pool().BeginTx(ctx, TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	creator := "child-creator"
	if _, err := s.InsertWorkflowStatus(ctx, InsertWorkflowStatusDBInput{
		Status: models.WorkflowStatus{
			ID: child, Status: models.WorkflowStatusEnqueued, Name: "child", QueueName: "q",
			ShardKey: childKey, ParentWorkflowID: parent, ParentShardKey: parentKey, CreatedAt: time.Now(),
		},
		Tx:         tx,
		CreatorXID: &creator,
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return s.RecordChildWorkflow(ctx, RecordChildWorkflowDBInput{
		ParentWorkflowID: parent, ParentShardKey: parentKey, ParentOwnerXID: owner,
		ChildWorkflowID: child, ChildShardKey: childKey, StepID: 0, StepName: "child", StartedAt: time.Now(),
	})
}

// A cancel with children of a parent that is starting a child under another shard key
// must leave no live child: either the cancel reaches the child, or the start fails
// and no child row is left to run.
func TestShardedCancelChildrenReachesCrossKeyChild(t *testing.T) {
	ctx := context.Background()
	const schema = "dbos_shard_cross_key_cancel_test"
	pool := shardedTestSchema(t, ctx, schema)
	s := &SysDB{pool: NewPgxPool(pool), dialect: PostgresDialect{}, schema: schema, sharded: true, logger: slog.New(slog.DiscardHandler)}

	const parent, parentKey, owner = "parent", "key-a", "parent-owner"
	const child, childKey = "child", "key-b"
	ctx = WithShardKeys(ctx, map[string]string{parent: parentKey, child: childKey})

	tx, err := s.Pool().BeginTx(ctx, TxOptions{})
	require.NoError(t, err)
	defer tx.Rollback(ctx) //nolint:errcheck
	ownerXID := owner
	_, err = s.InsertWorkflowStatus(ctx, InsertWorkflowStatusDBInput{
		Status:     models.WorkflowStatus{ID: parent, Status: models.WorkflowStatusPending, Name: "parent", ShardKey: parentKey, CreatedAt: time.Now()},
		Tx:         tx,
		CreatorXID: &ownerXID,
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	startErr := startCrossKeyChild(ctx, s, parent, parentKey, owner, child, childKey, func() {
		_, err := s.CancelWorkflows(WithShardKey(ctx, parent, parentKey), CancelWorkflowsDBInput{WorkflowIDs: []string{parent}, CancelChildren: true})
		require.NoError(t, err)
	})

	statuses := map[string]string{}
	rows, err := pool.Query(ctx, fmt.Sprintf(`SELECT workflow_uuid, status FROM %q.workflow_status`, schema))
	require.NoError(t, err)
	for rows.Next() {
		var id, status string
		require.NoError(t, rows.Scan(&id, &status))
		statuses[id] = status
	}
	require.NoError(t, rows.Err())

	require.Equal(t, string(models.WorkflowStatusCancelled), statuses[parent])
	if startErr != nil {
		var dbosErr *models.Error
		require.True(t, errors.As(startErr, &dbosErr) && dbosErr.Code == models.ErrorCodeConflictingID, "start error: %v", startErr)
		if status, live := statuses[child]; live {
			require.Equal(t, string(models.WorkflowStatusCancelled), status,
				"the child start failed (%v) but left the child %s under a cancelled parent", startErr, status)
		}
		return
	}
	require.Equal(t, string(models.WorkflowStatusCancelled), statuses[child], "the cancel missed the child")
}
