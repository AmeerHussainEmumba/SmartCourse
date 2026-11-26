//go:build integration

package enrollment_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
)

// Enroll/Withdraw requests whose context dies at a random moment inside the
// transaction (client disconnect, proxy timeout). The transaction must either
// commit completely or roll back completely: afterwards the seat invariants
// hold, no transaction is left open server-side, and the pool is not leaking.
func TestAdv_EnrollWithdraw_UnderRandomContextCancellation_AtomicAndLeakFree(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := enrollment.NewService(enrollment.NewRepository(env.DB))
	const seats = 12
	limit := seats
	courseID := seedCourse(t, env, &limit)
	sqlDB, _ := env.DB.DB()

	var wg sync.WaitGroup
	for w := 0; w < 40; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			sid := newStudentID()
			for i := 0; i < 25; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rng.Intn(4000))*time.Microsecond)
				e, err := svc.Enroll(ctx, sid, courseID)
				cancel()
				if err == nil && rng.Intn(2) == 0 {
					wctx, wcancel := context.WithTimeout(context.Background(), time.Duration(rng.Intn(4000))*time.Microsecond)
					_ = svc.Withdraw(wctx, sid, e.ID)
					wcancel()
				}
			}
		}(w)
	}
	wg.Wait()

	deadline := time.Now().Add(15 * time.Second)
	for {
		var idleTx, inUse int64
		env.DB.Raw(`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state LIKE 'idle in transaction%'`).Row().Scan(&idleTx)
		inUse = int64(sqlDB.Stats().InUse)
		if idleTx == 0 && inUse == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leak after cancellations: %d idle-in-transaction sessions, %d connections in use", idleTx, inUse)
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertSeatInvariants(t, env, courseID, seats)

	// And the system still enrolls normally once the storm is over.
	var free int64
	env.DB.Raw(`SELECT enrollment_limit - enrollment_count FROM courses WHERE id = ?`, courseID).Row().Scan(&free)
	if free > 0 {
		if _, err := svc.Enroll(context.Background(), uuid.Must(uuid.NewV7()), courseID); err != nil {
			t.Fatalf("enrollment after the storm: %v", err)
		}
	}
}
