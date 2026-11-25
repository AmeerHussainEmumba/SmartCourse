package jobs

import (
	"context"
	"errors"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/asynqx"
)

type nsScheduler struct {
	fakeScheduler
	ns asynqx.Namespace
}

func (s *nsScheduler) Namespace() asynqx.Namespace { return s.ns }

type nsRegistry struct {
	fakeRegistry
	ns asynqx.Namespace
}

func (r *nsRegistry) Namespace() asynqx.Namespace { return r.ns }

func TestRegister_RefusesSchedulerAndRegistryInDifferentNamespaces(t *testing.T) {
	j := Job{Name: "maintenance:x", Spec: "* * * * *", Run: func(context.Context) error { return nil }}
	err := Register(&nsScheduler{ns: "course"}, &nsRegistry{ns: "identity"}, 1, j)
	if !errors.Is(err, ErrNamespaceMismatch) {
		t.Fatalf("err = %v, want ErrNamespaceMismatch", err)
	}
	if err := Register(&nsScheduler{ns: "course"}, &nsRegistry{ns: "course"}, 1, j); err != nil {
		t.Fatalf("same namespace: %v", err)
	}
}
