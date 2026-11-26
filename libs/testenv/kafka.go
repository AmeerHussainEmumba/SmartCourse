package testenv

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// kafkaImage is the image docker-compose.yml pins for the kafka service, so
// the tests run against the broker the project runs. testcontainers-go's own
// kafka module is not used: it starts confluentinc/confluent-local through a
// Confluent-specific startup script and only validates that image family, so
// it cannot start apache/kafka.
const kafkaImage = "apache/kafka:3.7.0"

// KafkaEnv is a running single-node Kafka (KRaft) test container.
type KafkaEnv struct {
	// Brokers are the bootstrap addresses reachable from the test process.
	Brokers []string

	container testcontainers.Container
	addr      string
}

// SetupKafka starts a fresh Kafka broker and returns its addresses. Like
// Setup, each call gets its own container, which is terminated on test
// cleanup, and it is separate from Setup so tests that need no Kafka do not
// pay for the container.
//
// The broker is published on a fixed host port chosen at start (not an
// ephemeral one Docker may reassign), and advertises that address, so Stop
// followed by Start brings back the same address and the same data. Auto
// topic creation is left at the broker default (on); tests that care create
// their topics first.
func SetupKafka(t *testing.T) *KafkaEnv {
	t.Helper()
	ctx := context.Background()

	port, err := freePort()
	if err != nil {
		t.Fatalf("pick a free port for kafka: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        kafkaImage,
			ExposedPorts: []string{"9094/tcp"},
			// Bind the host port ourselves, so the address the broker
			// advertises stays valid (and survives a restart).
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = network.PortMap{
					network.MustParsePort("9094/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}},
				}
			},
			Env: map[string]string{
				"CLUSTER_ID":                                     "smartcourse-test-cluster1",
				"KAFKA_NODE_ID":                                  "1",
				"KAFKA_PROCESS_ROLES":                            "broker,controller",
				"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094",
				"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://localhost:9092,EXTERNAL://" + addr,
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			},
			WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	return &KafkaEnv{Brokers: []string{addr}, container: c, addr: addr}
}

// Stop stops the broker, keeping the container and its data, so a test can
// prove its code survives Kafka going away. Cleanup still terminates it.
func (k *KafkaEnv) Stop(t *testing.T) {
	t.Helper()
	if err := k.container.Stop(context.Background(), nil); err != nil {
		t.Fatalf("stop kafka container: %v", err)
	}
}

// Kill stops the broker with a zero grace period (SIGTERM, then SIGKILL at
// once), the closest a container test gets to a crash: the broker gets no
// chance to flush its producer-state snapshot or close its log cleanly. Data on
// disk is kept, and Start brings it back.
func (k *KafkaEnv) Kill(t *testing.T) {
	t.Helper()
	zero := 0 * time.Second
	if err := k.container.Stop(context.Background(), &zero); err != nil {
		t.Fatalf("kill kafka container: %v", err)
	}
}

// Start restarts a broker stopped with Stop on the same address, and returns
// once the address accepts connections. A TCP connect is a weaker signal than
// a metadata response (the listener can open slightly before the broker serves
// requests), so clients under test must retry, which a real client does anyway.
func (k *KafkaEnv) Start(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if err := k.container.Start(ctx); err != nil {
		t.Fatalf("restart kafka container: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", k.addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("kafka did not accept connections on %s after restart", k.addr)
}

// freePort asks the OS for a free TCP port. The port is released before the
// container binds it, so another process could in theory take it in between;
// SetupKafka then fails loudly at container start rather than misbehaving.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
