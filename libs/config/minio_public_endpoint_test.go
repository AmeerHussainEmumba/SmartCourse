package config

import "testing"

// MINIO_PUBLIC_ENDPOINT is optional: unset it is empty (callers fall back to
// MINIO_ENDPOINT), set it is carried through verbatim.
func TestLoad_MinIOPublicEndpoint(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MinIO.PublicEndpoint != "" {
		t.Fatalf("PublicEndpoint = %q, want empty when unset", cfg.MinIO.PublicEndpoint)
	}

	t.Setenv("MINIO_PUBLIC_ENDPOINT", "http://localhost:9002")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MinIO.PublicEndpoint != "http://localhost:9002" {
		t.Fatalf("PublicEndpoint = %q", cfg.MinIO.PublicEndpoint)
	}
}
