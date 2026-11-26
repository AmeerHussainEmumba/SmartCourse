//go:build live

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
)

// Runs against a real Schema Registry:
//
//	docker compose --profile events up -d kafka schema-registry
//	cd libs/events && GOWORK=off go test -tags live -run Live -v ./cmd/register-schemas
//
// SCHEMA_REGISTRY_URL overrides http://localhost:8081.
func liveClient(t *testing.T) *registry.Client {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	if url == "" {
		url = "http://localhost:8081"
	}
	return registry.New(url)
}

func TestLive_RegisterAllIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := liveClient(t)
	subjects, err := events.Subjects()
	if err != nil {
		t.Fatal(err)
	}

	var first, second bytes.Buffer
	if err := run(ctx, &first, c, subjects); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := run(ctx, &second, c, subjects); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if first.String() != second.String() {
		t.Errorf("second run changed ids or versions:\n%s\nvs\n%s", first.String(), second.String())
	}
	t.Logf("registered:\n%s", second.String())

	// The registry stores what we sent: fetch by id and compare.
	for _, s := range subjects {
		id, err := c.Register(ctx, s.Name, s.Schema)
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.SchemaByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Type != "PROTOBUF" {
			t.Errorf("%s: type %q", s.Name, got.Type)
		}
	}
}

func TestLive_RegistryRejectsIncompatibleChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := liveClient(t)
	subject := fmt.Sprintf("smartcourse.livetest.v1.T%d", time.Now().UnixNano())
	v1 := `syntax = "proto3";
package smartcourse.livetest.v1;
message T { string a = 1; }
`
	// Same field number, different wire type: breaks old data.
	bad := `syntax = "proto3";
package smartcourse.livetest.v1;
message T { int32 a = 1; }
`
	additive := `syntax = "proto3";
package smartcourse.livetest.v1;
message T { string a = 1; string b = 2; }
`
	if err := c.SetCompatibility(ctx, subject, registry.Backward); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Register(ctx, subject, v1); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.IsCompatible(ctx, subject, additive); err != nil || !ok {
		t.Fatalf("additive change should be compatible: ok=%v err=%v", ok, err)
	}
	if ok, err := c.IsCompatible(ctx, subject, bad); err != nil || ok {
		t.Fatalf("type change should be incompatible: ok=%v err=%v", ok, err)
	}
	var apiErr *registry.APIError
	if _, err := c.Register(ctx, subject, bad); !errors.As(err, &apiErr) || apiErr.Status != 409 {
		t.Fatalf("register of incompatible schema: %v", err)
	}
}
