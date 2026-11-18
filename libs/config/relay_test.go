package config

import (
	"strings"
	"testing"
)

func TestLoad_RelayPool_RequiresTwoConnectionsPerWorker(t *testing.T) {
	cases := []struct {
		name     string
		open     string
		workers  string
		wantFail bool
	}{
		{"defaults (20 conns, 4 workers)", "", "", false},
		{"exactly 2x", "8", "4", false},
		{"one short", "7", "4", true},
		{"pool equals workers", "4", "4", true},
		{"many workers, big pool", "128", "64", false},
		{"many workers, small pool", "20", "16", true},
		{"single worker needs two", "2", "1", false},
		{"single worker, one conn", "1", "1", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setValidEnv(t)
			if c.open != "" {
				t.Setenv("POSTGRES_MAX_OPEN_CONNS", c.open)
			}
			if c.workers != "" {
				t.Setenv("RELAY_WORKERS", c.workers)
			}
			_, err := Load()
			if c.wantFail {
				if err == nil || !strings.Contains(err.Error(), "POSTGRES_MAX_OPEN_CONNS") || !strings.Contains(err.Error(), "ADR-0027") {
					t.Fatalf("want a pool-size error naming ADR-0027, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoad_RelayWorkers_RangeAndDefault(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil || cfg.Relay.Workers != 4 {
		t.Fatalf("default workers = %+v, %v; want 4", cfg, err)
	}
	for _, v := range []string{"0", "-1", "65", "lots"} {
		setValidEnv(t)
		t.Setenv("RELAY_WORKERS", v)
		if _, err := Load(); err == nil {
			t.Errorf("Load accepted RELAY_WORKERS=%s", v)
		}
	}
}
