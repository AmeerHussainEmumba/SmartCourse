package config

import (
	"strings"
	"testing"
)

func TestLoad_Telemetry_DefaultsAreSafe(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	tel := cfg.Telemetry
	if tel.TracingEnabled() {
		t.Error("tracing must be off when no endpoint is set")
	}
	if tel.SampleRatio != 1 || !tel.MetricsEnabled || tel.MetricsAddr != ":9100" || tel.ServiceName != "" {
		t.Errorf("defaults = %+v", tel)
	}
}

func TestLoad_Telemetry_EndpointForms(t *testing.T) {
	cases := []struct {
		name, endpoint string
		disabled       string
		wantErr        string // substring; "" means valid
		wantEnabled    bool
	}{
		{"unset", "", "", "", false},
		{"host:port", "jaeger:4318", "", "", true},
		{"http url", "http://jaeger:4318", "", "", true},
		{"https url", "https://collector.example:4318/v1/traces", "", "", true},
		{"sdk disabled wins", "jaeger:4318", "true", "", false},
		{"grpc scheme refused", "grpc://jaeger:4317", "", "http:// or https://", false},
		{"no port", "jaeger", "", "host:port", false},
		{"bad port", "jaeger:99999", "", "port", false},
		{"no host", "http://:4318", "", "name a host", false},
		{"credentials refused", "http://user:secret@jaeger:4318", "", "credentials", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", c.endpoint)
			t.Setenv("OTEL_SDK_DISABLED", c.disabled)
			cfg, err := Load()
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want error containing %q, got %v", c.wantErr, err)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatalf("error echoes the credential: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Telemetry.TracingEnabled() != c.wantEnabled {
				t.Errorf("TracingEnabled = %v, want %v", cfg.Telemetry.TracingEnabled(), c.wantEnabled)
			}
		})
	}
}

func TestLoad_Telemetry_SamplerAndAddr(t *testing.T) {
	for _, v := range []string{"-0.1", "1.5", "NaN", "lots"} {
		setValidEnv(t)
		t.Setenv("OTEL_TRACES_SAMPLER_ARG", v)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "OTEL_TRACES_SAMPLER_ARG") {
			t.Errorf("Load accepted OTEL_TRACES_SAMPLER_ARG=%s (err %v)", v, err)
		}
	}
	setValidEnv(t)
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	t.Setenv("METRICS_HTTP_ADDR", "127.0.0.1:9200")
	t.Setenv("METRICS_ENABLED", "false")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Telemetry.SampleRatio != 0.25 || cfg.Telemetry.MetricsAddr != "127.0.0.1:9200" || cfg.Telemetry.MetricsEnabled {
		t.Errorf("got %+v", cfg.Telemetry)
	}
	for _, v := range []string{"9100", ":0", ":99999", "bad host:9100"} {
		setValidEnv(t)
		t.Setenv("METRICS_HTTP_ADDR", v)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "METRICS_HTTP_ADDR") {
			t.Errorf("Load accepted METRICS_HTTP_ADDR=%q (err %v)", v, err)
		}
	}
	setValidEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "has space")
	if _, err := Load(); err == nil {
		t.Error("Load accepted a service name with whitespace")
	}
}
