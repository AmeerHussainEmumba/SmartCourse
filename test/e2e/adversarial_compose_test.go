package e2e_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Static review of docker-compose.yml (security tester, Week 3). Every
// published port is bound to 127.0.0.1, and no image is a floating tag (S-13,
// fixed: was a failing reproducer).
//
//	cd test/e2e && go test -run Compose ./

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

// S-13 (fixed): images pulled as :latest (MinIO server and client, Temporal
// server and UI) were not reproducible and let an upstream change or compromise
// reach every developer machine and the container E2E harness on the next
// pull. Every image must name a tag other than "latest" (an untagged image is
// "latest" too), in the compose file and in every container the test harness
// and the testcontainers helpers start.
func TestCompose_ImagesAreNotFloatingTags(t *testing.T) {
	for _, line := range strings.Split(readCompose(t), "\n") {
		l := strings.TrimSpace(line)
		if img, ok := strings.CutPrefix(l, "image:"); ok {
			checkPinned(t, "docker-compose.yml", strings.TrimSpace(img))
		}
	}
}

// The harness and the shared testcontainers helpers pin the same images as
// compose: a floating tag there reaches CI and every developer's test run.
func TestHarnessImagesAreNotFloatingTags(t *testing.T) {
	imageConst := regexp.MustCompile(`(?m)^\s*(?:const\s+)?\w*[iI]mage\w*\s*=\s*"([^"]+)"`)
	for _, f := range []string{
		"harness/infra.go", "harness/mailpit.go", "harness/jaeger.go", "harness/mongocontainer.go",
		"../../libs/testenv/kafka.go", "../../services/course/internal/miniotest/miniotest.go",
	} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		found := imageConst.FindAllStringSubmatch(string(b), -1)
		if len(found) == 0 {
			t.Errorf("%s: no image constant found (did the helper move? update this test)", f)
		}
		for _, m := range found {
			checkPinned(t, f, m[1])
		}
	}
}

func checkPinned(t *testing.T, where, image string) {
	t.Helper()
	ref, _, _ := strings.Cut(image, "@") // a digest pin is fine next to a tag
	name := ref[strings.LastIndex(ref, "/")+1:]
	_, tag, hasTag := strings.Cut(name, ":")
	switch {
	case !hasTag || tag == "":
		t.Errorf("%s: image %q has no tag (floats to latest)", where, image)
	case tag == "latest":
		t.Errorf("%s: floating tag: %s", where, image)
	}
}
