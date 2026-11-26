package harness

import (
	"testing"
	"time"

	"gorm.io/gorm"
)

// RowLock is a Postgres row lock held by the test, used to stop a service's
// transaction at a known point without a hook in the service: while the test
// holds `SELECT ... FOR UPDATE` on a row the handler needs, the handler's
// transaction blocks on it, with its earlier writes uncommitted. The scenario
// can then kill the process at exactly that point (E6).
type RowLock struct {
	tx *gorm.DB
}

// HoldRowLock opens a transaction on the test's connection and runs lockQuery
// (a `SELECT ... FOR UPDATE`) in it. Release rolls the transaction back, and
// is also registered as test cleanup, so a failed test never leaves the row
// locked.
func (s *Stack) HoldRowLock(t testing.TB, lockQuery string, args ...any) *RowLock {
	t.Helper()
	tx := s.Env.DB.Begin()
	if tx.Error != nil {
		t.Fatalf("begin lock transaction: %v", tx.Error)
	}
	var one int
	if err := tx.Raw(lockQuery, args...).Row().Scan(&one); err != nil {
		_ = tx.Rollback().Error
		t.Fatalf("take row lock (%s): %v", lockQuery, err)
	}
	l := &RowLock{tx: tx}
	t.Cleanup(l.Release)
	return l
}

// Release rolls the lock transaction back (idempotent).
func (l *RowLock) Release() {
	if l.tx != nil {
		_ = l.tx.Rollback().Error
		l.tx = nil
	}
}

// WaitBlockedOn waits until some other backend is waiting for a lock in a
// query whose text contains fragment (case-insensitive), the sign that the
// service's transaction has reached the locked statement.
func (s *Stack) WaitBlockedOn(t testing.TB, fragment string, timeout time.Duration) {
	t.Helper()
	Eventually(t, timeout, "a backend blocked on a lock in a query containing "+fragment, func() error {
		n, err := s.Count(`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query ILIKE ? AND pid <> pg_backend_pid()`, "%"+fragment+"%")
		if err != nil {
			return err
		}
		if n == 0 {
			return Errf("no backend is waiting on %q", fragment)
		}
		return nil
	})
}

// WaitNoBlockedBackends waits until no backend is waiting for a lock: after a
// killed service's connection was released from its lock wait and aborted.
func (s *Stack) WaitNoBlockedBackends(t testing.TB, timeout time.Duration) {
	t.Helper()
	Eventually(t, timeout, "no backend waiting on a lock", func() error {
		n, err := s.Count(`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND pid <> pg_backend_pid()`)
		if err != nil {
			return err
		}
		if n != 0 {
			return Errf("%d backends still waiting on a lock", n)
		}
		return nil
	})
}
