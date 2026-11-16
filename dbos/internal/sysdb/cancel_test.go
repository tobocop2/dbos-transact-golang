package sysdb

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	sqlitelib "modernc.org/sqlite"
)

// childReadHookPool runs afterRead once, when the rows of the first query that
// lists workflows by parent are closed, on the pool or on any transaction it
// begins. That is the point between the read of the children and the write
// that cancels them.
type childReadHookPool struct {
	Pool
	afterRead func()
}

func (p *childReadHookPool) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	rows, err := p.Pool.Query(ctx, q, args...)
	return p.wrap(q, rows, err)
}

func (p *childReadHookPool) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	tx, err := p.Pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &childReadHookTx{Tx: tx, pool: p}, nil
}

// wrap arms the hook on the rows of the first query that filters on the parent.
func (p *childReadHookPool) wrap(q string, rows Rows, err error) (Rows, error) {
	if err != nil || p.afterRead == nil || !strings.Contains(q, "parent_workflow_id IN") && !strings.Contains(q, "parent_workflow_id =") {
		return rows, err
	}
	hook := p.afterRead
	p.afterRead = nil
	return &childReadHookRows{Rows: rows, afterClose: hook}, nil
}

type childReadHookTx struct {
	Tx
	pool *childReadHookPool
}

func (t *childReadHookTx) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	rows, err := t.Tx.Query(ctx, q, args...)
	return t.pool.wrap(q, rows, err)
}

type childReadHookRows struct {
	Rows
	afterClose func()
}

func (r *childReadHookRows) Close() error {
	err := r.Rows.Close()
	if r.afterClose != nil {
		hook := r.afterClose
		r.afterClose = nil
		hook()
	}
	return err
}

func registerTestSQLiteDriver() {
	RegisterSQLiteDriver(SQLiteDriver{
		DriverName: "sqlite",
		ErrorCode: func(err error) int {
			var se *sqlitelib.Error
			if errors.As(err, &se) {
				return se.Code()
			}
			return -1
		},
	})
}

// startTestChild starts childID under parentID the way a parent workflow
// starts a child: the child row and the parent's owner check commit in one
// transaction.
func startTestChild(ctx context.Context, s *SysDB, parentID, parentXID, childID string) error {
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	childXID := "child-xid"
	if _, err := s.InsertWorkflowStatus(ctx, InsertWorkflowStatusDBInput{
		Status: models.WorkflowStatus{
			ID:               childID,
			Status:           models.WorkflowStatusPending,
			Name:             "child",
			ParentWorkflowID: parentID,
		},
		Tx:         tx,
		CreatorXID: &childXID,
	}); err != nil {
		return err
	}
	if err := s.RecordChildWorkflow(ctx, RecordChildWorkflowDBInput{
		ParentWorkflowID: parentID,
		ParentOwnerXID:   parentXID,
		ChildWorkflowID:  childID,
		StepID:           0,
		StepName:         "child",
		StartedAt:        time.Now(),
		Tx:               tx,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// A child that starts while its parent is being cancelled with its children
// must not keep running under the cancelled parent: either the cancel lists it
// and cancels it, or its start fails the parent's owner check.
func TestCancelWorkflowsCancelsAChildThatStartsDuringTheCancel(t *testing.T) {
	registerTestSQLiteDriver()
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)
	path := filepath.Join(t.TempDir(), "cancel.db")

	opened, err := newSqliteSystemDatabase(nil, ctx, "sqlite:"+path, "dbos", nil, logger, "", false)
	if err != nil {
		t.Fatal(err)
	}
	s := opened.(*SysDB)
	t.Cleanup(s.pool.Close)

	// A second handle that never waits for the write lock, so the hook sees
	// at once whether the cancel holds it.
	noWaitDB, err := sql.Open("sqlite", path+"?_txlock=immediate&_pragma=busy_timeout(0)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = noWaitDB.Close() })
	starter := &SysDB{pool: NewSQLPool(noWaitDB), dialect: SqliteDialect{}, schema: s.schema, logger: logger}

	const parentID, parentXID, childID = "parent", "parent-xid", "child"
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertWorkflowStatus(ctx, InsertWorkflowStatusDBInput{
		Status:     models.WorkflowStatus{ID: parentID, Status: models.WorkflowStatusPending, Name: "parent"},
		Tx:         tx,
		CreatorXID: func() *string { x := parentXID; return &x }(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	hooked := &childReadHookPool{Pool: s.pool}
	s.pool = hooked
	hookRan, startDeferred := false, false
	hooked.afterRead = func() {
		hookRan = true
		err := startTestChild(ctx, starter, parentID, parentXID, childID)
		if err != nil && starter.dialect.IsRetryable(err, nil) {
			// The cancel holds the write lock: the start runs after it.
			startDeferred = true
			return
		}
		if err != nil {
			t.Errorf("child start between the read and the cancel: %v", err)
		}
	}

	if _, err := s.CancelWorkflows(ctx, CancelWorkflowsDBInput{CancelChildren: true, WorkflowIDs: []string{parentID}}); err != nil {
		t.Fatal(err)
	}
	if !hookRan {
		t.Fatal("the cancel did not read the children of the parent")
	}
	t.Logf("child start waited for the cancel: %v", startDeferred)
	if startDeferred {
		err := startTestChild(ctx, s, parentID, parentXID, childID)
		var dbosErr *models.Error
		if !errors.As(err, &dbosErr) || dbosErr.Code != models.ErrorCodeConflictingID {
			t.Fatalf("child start after the cancel: got %v, want a conflicting ID error", err)
		}
	}

	statuses, err := s.ListWorkflows(ctx, ListWorkflowsDBInput{WorkflowIDs: []string{parentID, childID}})
	if err != nil {
		t.Fatal(err)
	}
	foundParent := false
	for _, st := range statuses {
		foundParent = foundParent || st.ID == parentID
		if st.Status != models.WorkflowStatusCancelled {
			t.Errorf("workflow %s is %s after the cancel of its parent, want %s", st.ID, st.Status, models.WorkflowStatusCancelled)
		}
	}
	if !foundParent {
		t.Errorf("parent %s not found after the cancel", parentID)
	}
}
