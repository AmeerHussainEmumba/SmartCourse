package harness

import (
	"context"
	"fmt"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// mongoImage is the image libs/testenv and docker-compose.yml use.
const mongoImage = "mongo:7"

// mongoContainer is the scenario's MongoDB. Unlike testenv.SetupMongo's it
// publishes a fixed host port, so it can be stopped and started again on the
// same address (E13: MongoDB down while the pipeline keeps working).
type mongoContainer struct {
	URI       string
	container testcontainers.Container
}

func startMongo(t testing.TB) *mongoContainer {
	t.Helper()
	port, err := freePort()
	if err != nil {
		t.Fatalf("pick a port for mongodb: %v", err)
	}
	c, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        mongoImage,
			ExposedPorts: []string{"27017/tcp"},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = network.PortMap{
					network.MustParsePort("27017/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}},
				}
			},
			WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if c != nil {
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	}
	if err != nil {
		t.Fatalf("start mongodb container: %v", err)
	}
	return &mongoContainer{URI: fmt.Sprintf("mongodb://127.0.0.1:%d", port), container: c}
}

// StopMongo stops MongoDB (data kept); StartMongo brings it back on the same
// address and waits until it accepts queries.
func (s *Stack) StopMongo(t testing.TB) {
	t.Helper()
	if err := s.mongoC.container.Stop(context.Background(), nil); err != nil {
		t.Fatalf("stop mongodb: %v", err)
	}
}

// StartMongo restarts the MongoDB stopped by StopMongo.
func (s *Stack) StartMongo(t testing.TB) {
	t.Helper()
	if err := s.mongoC.container.Start(context.Background()); err != nil {
		t.Fatalf("restart mongodb: %v", err)
	}
	Eventually(t, time.Minute, "mongodb to answer after the restart", func() error {
		_, err := s.Mongo.Count("events", nil)
		return err
	})
}
