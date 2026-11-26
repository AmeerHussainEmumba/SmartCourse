package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// jaegerImage is the image docker-compose.yml pins for the trace backend.
const jaegerImage = "jaegertracing/all-in-one:1.76.0"

// Jaeger is the trace backend of a Stack started with Options.Tracing: the
// four binaries export their spans to its OTLP/HTTP port (the same
// OTEL_EXPORTER_OTLP_ENDPOINT variable compose sets), and a scenario reads
// them back through its query API.
type Jaeger struct {
	OTLPEndpoint string // host:port of OTLP/HTTP, what the binaries are given
	QueryURL     string // base URL of the query API and UI
}

// Span is one span as Jaeger's /api/traces returns it, with the parts a
// scenario asserts on resolved: the service that exported it, its tags and
// its parent.
type Span struct {
	TraceID   string
	SpanID    string
	ParentID  string // empty for a span with no parent reference
	Operation string
	Service   string
	Tags      map[string]string
}

// Kind is the span.kind tag ("server", "producer", "consumer", "internal"...).
func (s Span) Kind() string { return s.Tags["span.kind"] }

// Tag is the value of tag key, "" when absent.
func (s Span) Tag(key string) string { return s.Tags[key] }

// Errored reports whether the span carries the error tag Jaeger derives from
// an Error status.
func (s Span) Errored() bool { return s.Tags["error"] == "true" }

func startJaeger(t testing.TB) *Jaeger {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        jaegerImage,
			ExposedPorts: []string{"16686/tcp", "4318/tcp"},
			Env:          map[string]string{"COLLECTOR_OTLP_ENABLED": "true"},
			WaitingFor:   wait.ForHTTP("/api/services").WithPort("16686/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if c != nil {
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	}
	if err != nil {
		t.Fatalf("start jaeger container: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("jaeger host: %v", err)
	}
	query, err := c.MappedPort(ctx, "16686/tcp")
	if err != nil {
		t.Fatalf("jaeger query port: %v", err)
	}
	otlp, err := c.MappedPort(ctx, "4318/tcp")
	if err != nil {
		t.Fatalf("jaeger otlp port: %v", err)
	}
	return &Jaeger{
		OTLPEndpoint: fmt.Sprintf("%s:%s", host, otlp.Port()),
		QueryURL:     fmt.Sprintf("http://%s:%s", host, query.Port()),
	}
}

type jaegerResponse struct {
	Data []struct {
		TraceID string `json:"traceID"`
		Spans   []struct {
			TraceID       string `json:"traceID"`
			SpanID        string `json:"spanID"`
			OperationName string `json:"operationName"`
			ProcessID     string `json:"processID"`
			References    []struct {
				RefType string `json:"refType"`
				SpanID  string `json:"spanID"`
			} `json:"references"`
			Tags []struct {
				Key   string `json:"key"`
				Value any    `json:"value"`
			} `json:"tags"`
		} `json:"spans"`
		Processes map[string]struct {
			ServiceName string `json:"serviceName"`
		} `json:"processes"`
	} `json:"data"`
}

func (j *Jaeger) get(path string) (*jaegerResponse, int, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(j.QueryURL + path)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, nil
	}
	var out jaegerResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("decode jaeger response: %w", err)
	}
	return &out, resp.StatusCode, nil
}

func (r *jaegerResponse) spans() []Span {
	var out []Span
	for _, tr := range r.Data {
		for _, sp := range tr.Spans {
			s := Span{TraceID: sp.TraceID, SpanID: sp.SpanID, Operation: sp.OperationName, Tags: map[string]string{}}
			s.Service = tr.Processes[sp.ProcessID].ServiceName
			for _, ref := range sp.References {
				if ref.RefType == "CHILD_OF" || s.ParentID == "" {
					s.ParentID = ref.SpanID
				}
			}
			for _, tag := range sp.Tags {
				s.Tags[tag.Key] = fmt.Sprint(tag.Value)
			}
			out = append(out, s)
		}
	}
	return out
}

// Trace returns the spans Jaeger holds for traceID (empty when it has none
// yet: spans are exported in batches, so poll).
func (j *Jaeger) Trace(traceID string) ([]Span, error) {
	r, code, err := j.get("/api/traces/" + url.PathEscape(traceID))
	if err != nil {
		return nil, err
	}
	if code == http.StatusNotFound {
		return nil, nil
	}
	if r == nil {
		return nil, fmt.Errorf("jaeger GET /api/traces/%s: HTTP %d", traceID, code)
	}
	return r.spans(), nil
}

// FindErrored lists the spans of service that ended in error, from the last
// hour (Jaeger's tag search: error=true).
func (j *Jaeger) FindErrored(service string) ([]Span, error) {
	q := url.Values{"service": {service}, "tags": {`{"error":"true"}`}, "lookback": {"1h"}, "limit": {"100"}}
	r, code, err := j.get("/api/traces?" + q.Encode())
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("jaeger GET /api/traces: HTTP %d", code)
	}
	return r.spans(), nil
}
