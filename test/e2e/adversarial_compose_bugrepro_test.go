//go:build bugrepro

package e2e_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Static review of docker-compose.yml (security tester, Week 3). Held: every
// published port is bound to 127.0.0.1. Bugrepro: images are floating tags.
//
//	cd test/e2e && go test -tags=bugrepro -run Compose ./

func readCompose(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// HELD: every host port mapping (inline and list form) names 127.0.0.1, so no
// auth-less datastore (Postgres, Redis, Mongo, Kafka, Schema Registry, MinIO
// with default credentials, Temporal, Mailpit) is reachable from the LAN.
func TestAdversarial_Compose_EveryPublishedPortIsLoopbackOnly(t *testing.T) {
	c := readCompose(t)
	inline := regexp.MustCompile(`ports:\s*\[([^\]]*)\]`)
	for _, m := range inline.FindAllStringSubmatch(c, -1) {
		for _, p := range strings.Split(m[1], ",") {
			if p = strings.Trim(strings.TrimSpace(p), `"'`); p != "" && !strings.HasPrefix(p, "127.0.0.1:") {
				t.Errorf("port mapping %q is not bound to 127.0.0.1", p)
			}
		}
	}
	list := regexp.MustCompile(`(?m)^\s+- "?(\d{2,5}:\d{2,5}|0\.0\.0\.0:[^"\s]+)"?`)
	for _, m := range list.FindAllStringSubmatch(c, -1) {
		t.Errorf("list-form port mapping %q is not bound to 127.0.0.1", m[1])
	}
}

// S-13: images pulled as :latest (MinIO server and client, Temporal server and
// UI) are not reproducible and let an upstream change or compromise reach every
// developer machine and the container E2E harness on the next pull. Every other
// image in the file is pinned.
func TestBugrepro_Compose_ImagesAreNotFloatingTags(t *testing.T) {
	for _, line := range strings.Split(readCompose(t), "\n") {
		l := strings.TrimSpace(line)
		if strings.HasPrefix(l, "image:") && strings.HasSuffix(l, ":latest") {
			t.Errorf("floating tag: %s", l)
		}
	}
}
