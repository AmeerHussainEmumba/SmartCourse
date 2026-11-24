package harness

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/api/workflowservice/v1"
	temporalclient "go.temporal.io/sdk/client"
)

// Images are the ones docker-compose.yml pins, so the scenarios run against
// what the project runs. (Kafka, Postgres, Redis, MongoDB and Mailpit are
// pinned in libs/testenv and in mailpit.go.)
const (
	schemaRegistryImage = "confluentinc/cp-schema-registry:7.6.0"
	registryKafkaImage  = "apache/kafka:3.7.0"
	temporalImage       = "temporalio/auto-setup:latest"
	temporalDBImage     = "postgres:16-alpine"
)

// SharedInfra is what every scenario in the test binary reuses: the Schema
// Registry (with the Kafka broker that stores its schemas) and Temporal (with
// its own Postgres). Starting them costs 30 to 60 seconds, they hold nothing a
// scenario mutates (schemas are registered once and are idempotent, and each
// Stack uses its own Temporal task queue), so one set serves the whole run and
// is stopped by Main when the tests end.
//
// The registry gets a Kafka of its own, not the scenario's broker, on purpose:
// libs/testenv.SetupKafka advertises localhost to the broker's own clients,
// which another container cannot reach, and a scenario stops its broker
// (E5), which must not take the registry down with it.
type SharedInfra struct {
	RegistryURL  string // reachable from the test process and from subprocesses
	TemporalAddr string // host:port of the Temporal frontend
}

var (
	sharedOnce sync.Once
	shared     *SharedInfra
	sharedErr  error

	sharedMu   sync.Mutex
	sharedStop []func()
)

// Shared starts the shared infrastructure on first use and returns it.
func Shared(t testing.TB) *SharedInfra {
	t.Helper()
	sharedOnce.Do(func() { shared, sharedErr = startShared(t) })
	if sharedErr != nil {
		t.Fatalf("start shared infrastructure (schema registry, temporal): %v", sharedErr)
	}
	return shared
}

// Main is for a package's TestMain: it runs the tests, then stops the shared
// containers (testcontainers' reaper removes them anyway if the process dies).
//
//	func TestMain(m *testing.M) { os.Exit(harness.Main(m)) }
func Main(m interface{ Run() int }) int {
	code := m.Run()
	shutdownShared()
	return code
}

func onShutdown(f func()) {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	sharedStop = append(sharedStop, f)
}

func shutdownShared() {
	sharedMu.Lock()
	stops := sharedStop
	sharedStop = nil
	sharedMu.Unlock()
	for i := len(stops) - 1; i >= 0; i-- {
		stops[i]()
	}
}

func startShared(t testing.TB) (*SharedInfra, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	nw, err := network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create docker network: %w", err)
	}
	onShutdown(func() { _ = nw.Remove(context.Background()) })

	run := func(name, alias string, req testcontainers.ContainerRequest) (testcontainers.Container, error) {
		req.Networks = []string{nw.Name}
		if alias != "" {
			req.NetworkAliases = map[string][]string{nw.Name: {alias}}
		}
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
		if c != nil {
			onShutdown(func() { _ = c.Terminate(context.Background()) })
		}
		if err != nil {
			return nil, fmt.Errorf("start %s: %w", name, err)
		}
		return c, nil
	}

	si := &SharedInfra{}
	var wg sync.WaitGroup
	var regErr, temErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		si.RegistryURL, regErr = startRegistry(ctx, run)
	}()
	go func() {
		defer wg.Done()
		si.TemporalAddr, temErr = startTemporal(ctx, run)
	}()
	wg.Wait()
	if regErr != nil {
		return nil, regErr
	}
	if temErr != nil {
		return nil, temErr
	}

	// Register every event schema with the real registry, with the real tool
	// (libs/events/cmd/register-schemas), as a deploy does.
	var out bytes.Buffer
	reg := exec.CommandContext(ctx, Binary(t, "register-schemas"), "-registry", si.RegistryURL)
	reg.Stdout, reg.Stderr = &out, &out
	if err := reg.Run(); err != nil {
		return nil, fmt.Errorf("register-schemas against %s: %w\n%s", si.RegistryURL, err, out.String())
	}
	return si, nil
}

type runFunc func(name, alias string, req testcontainers.ContainerRequest) (testcontainers.Container, error)

// startRegistry starts the registry's private Kafka and the registry.
func startRegistry(ctx context.Context, run runFunc) (string, error) {
	_, err := run("schema registry's kafka", "sr-kafka", testcontainers.ContainerRequest{
		Image: registryKafkaImage,
		Env: map[string]string{
			"CLUSTER_ID":                                     "smartcourse-sr-cluster01",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093",
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://sr-kafka:9092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_HEAP_OPTS":                                "-Xmx384m -Xms128m",
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(2 * time.Minute),
	})
	if err != nil {
		return "", err
	}
	c, err := run("schema registry", "", testcontainers.ContainerRequest{
		Image:        schemaRegistryImage,
		ExposedPorts: []string{"8081/tcp"},
		Env: map[string]string{
			"SCHEMA_REGISTRY_HOST_NAME":                    "schema-registry",
			"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "sr-kafka:9092",
			"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8081",
			"SCHEMA_REGISTRY_HEAP_OPTS":                    "-Xmx256m -Xms64m",
		},
		WaitingFor: wait.ForHTTP("/subjects").WithPort("8081/tcp").WithStartupTimeout(3 * time.Minute),
	})
	if err != nil {
		return "", err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := c.MappedPort(ctx, "8081/tcp")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port()), nil
}

// startTemporal starts Temporal (the image compose uses, with its Postgres
// persistence in a small database container of its own) and waits until the
// "default" namespace exists: the image creates it a little after the port
// opens, and a publish before that fails.
func startTemporal(ctx context.Context, run runFunc) (string, error) {
	_, err := run("temporal's postgres", "temporal-pg", testcontainers.ContainerRequest{
		Image: temporalDBImage,
		Env: map[string]string{
			"POSTGRES_USER":     "temporal",
			"POSTGRES_PASSWORD": "temporal",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute),
	})
	if err != nil {
		return "", err
	}
	c, err := run("temporal", "", testcontainers.ContainerRequest{
		Image:        temporalImage,
		ExposedPorts: []string{"7233/tcp"},
		Env: map[string]string{
			"DB":             "postgres12",
			"DB_PORT":        "5432",
			"POSTGRES_USER":  "temporal",
			"POSTGRES_PWD":   "temporal",
			"POSTGRES_SEEDS": "temporal-pg",
		},
		WaitingFor: wait.ForListeningPort("7233/tcp").WithStartupTimeout(3 * time.Minute),
	})
	if err != nil {
		return "", err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := c.MappedPort(ctx, "7233/tcp")
	if err != nil {
		return "", err
	}
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	err = WaitFor(2*time.Minute, func() error {
		tc, err := temporalclient.Dial(temporalclient.Options{HostPort: addr})
		if err != nil {
			return err
		}
		defer tc.Close()
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = tc.WorkflowService().DescribeNamespace(dctx, &workflowservice.DescribeNamespaceRequest{Namespace: "default"})
		return err
	})
	if err != nil {
		return "", fmt.Errorf("temporal namespace default not ready at %s: %w", addr, err)
	}
	return addr, nil
}
