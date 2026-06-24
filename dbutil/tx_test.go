package dbutil_test

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/dbutil"
)

// --- minimal in-memory database/sql driver (no external deps) ---

type fakeConn struct {
	beginErr   error
	committed  bool
	rolledBack bool
}

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) {
	if c.beginErr != nil {
		return nil, c.beginErr
	}
	return &fakeTx{conn: c}, nil
}

type fakeTx struct{ conn *fakeConn }

func (t *fakeTx) Commit() error   { t.conn.committed = true; return nil }
func (t *fakeTx) Rollback() error { t.conn.rolledBack = true; return nil }

type fakeDriver struct {
	beginErr error
	mu       sync.Mutex
	last     *fakeConn
}

func (d *fakeDriver) Open(string) (driver.Conn, error) {
	c := &fakeConn{beginErr: d.beginErr}
	d.mu.Lock()
	d.last = c
	d.mu.Unlock()
	return c, nil
}

func (d *fakeDriver) lastConn() *fakeConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.last
}

var (
	okDriver       = &fakeDriver{}
	beginErrDriver = &fakeDriver{beginErr: errors.New("cannot begin")}
	registerOnce   sync.Once
)

func registerFakeDrivers() {
	// sql.Register panics on duplicate names, so register only once even when
	// the test binary runs with -count > 1.
	registerOnce.Do(func() {
		sql.Register("fakedb_ok", okDriver)
		sql.Register("fakedb_beginerr", beginErrDriver)
	})
}

func openFake(t *testing.T, name string) *sql.DB {
	t.Helper()
	registerFakeDrivers()
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// --- tests ---

func TestWrapTx_CommitOnSuccess(t *testing.T) {
	db := openFake(t, "fakedb_ok")

	require.NoError(t, dbutil.WrapTx(t.Context(), db, func(*sql.Tx) error {
		return nil
	}))

	conn := okDriver.lastConn()
	require.NotNil(t, conn)
	assert.True(t, conn.committed, "successful txFunc must commit")
	assert.False(t, conn.rolledBack)
}

func TestWrapTx_RollbackOnError(t *testing.T) {
	db := openFake(t, "fakedb_ok")

	sentinel := errors.New("boom")
	err := dbutil.WrapTx(t.Context(), db, func(*sql.Tx) error {
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	conn := okDriver.lastConn()
	require.NotNil(t, conn)
	assert.False(t, conn.committed)
	assert.True(t, conn.rolledBack, "failed txFunc must roll back")
}

func TestWrapTx_BeginError(t *testing.T) {
	db := openFake(t, "fakedb_beginerr")

	ran := false
	err := dbutil.WrapTx(t.Context(), db, func(*sql.Tx) error {
		ran = true
		return nil
	})
	require.Error(t, err)
	assert.False(t, ran, "txFunc must not run when Begin fails")
}
