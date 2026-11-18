package testenv

import (
	"context"
	"testing"

	tcmongodb "github.com/testcontainers/testcontainers-go/modules/mongodb"
)

// MongoEnv is a running MongoDB test container.
type MongoEnv struct {
	// URI is the connection string for the container (no credentials).
	URI string

	container *tcmongodb.MongoDBContainer
}

// SetupMongo starts a fresh MongoDB container and returns its connection
// URI. Like Setup, each call gets its own container, which is terminated on
// test cleanup. It is separate from Setup so tests that need no MongoDB do
// not pay for the container.
func SetupMongo(t *testing.T) *MongoEnv {
	t.Helper()
	ctx := context.Background()

	c, err := tcmongodb.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatalf("start mongodb container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	uri, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get mongodb connection string: %v", err)
	}
	return &MongoEnv{URI: uri, container: c}
}

// Stop stops the container, so a test can prove its code fails fast when
// MongoDB goes away. Cleanup still terminates it afterwards.
func (m *MongoEnv) Stop(t *testing.T) {
	t.Helper()
	if err := m.container.Stop(context.Background(), nil); err != nil {
		t.Fatalf("stop mongodb container: %v", err)
	}
}
