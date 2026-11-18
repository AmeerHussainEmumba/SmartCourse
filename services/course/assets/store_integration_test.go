//go:build integration

package assets_test

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/services/course/assets"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/internal/miniotest"
)

// What this checks, against a real MinIO: the pre-signed PUT really enforces
// the content type and length that were signed into it, expires, cannot be
// pointed at another key, and Stat reports existence, size and type.

func newStore(t *testing.T) (*assets.Store, *miniotest.Env) {
	t.Helper()
	env := miniotest.Setup(t)
	st, err := assets.New(env.Config)
	if err != nil {
		t.Fatal(err)
	}
	return st, env
}

const key = "courses/0190f0b0-0000-7000-8000-000000000001/assets/0190f0b0-0000-7000-8000-000000000002.pdf"

func TestStore_UploadThenStat(t *testing.T) {
	st, _ := newStore(t)
	ctx := context.Background()
	body := []byte("%PDF-1.4 hello")
	up, err := st.PresignUpload(ctx, key, "application/pdf", int64(len(body)), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := miniotest.Upload(t, up.URL, up.Headers, body); got != 200 {
		t.Fatalf("upload status = %d, want 200", got)
	}
	info, err := st.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(body)) || info.ContentType != "application/pdf" {
		t.Fatalf("Stat = %+v", info)
	}
	if !up.ExpiresAt.After(time.Now()) {
		t.Fatalf("ExpiresAt %v is not in the future", up.ExpiresAt)
	}
}

func TestStore_StatMissingIsErrAssetNotFound(t *testing.T) {
	st, _ := newStore(t)
	_, err := st.Stat(context.Background(), key)
	if !errors.Is(err, course.ErrAssetNotFound) {
		t.Fatalf("err = %v, want ErrAssetNotFound", err)
	}
}

func TestStore_PresignedURLRejectsWrongSizeAndWrongType(t *testing.T) {
	st, _ := newStore(t)
	ctx := context.Background()
	up, err := st.PresignUpload(ctx, key, "application/pdf", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// The signature covers the declared length: more or fewer bytes is refused.
	if got := miniotest.Upload(t, up.URL, up.Headers, []byte(strings.Repeat("x", 11))); got < 400 {
		t.Errorf("oversize body accepted: status %d", got)
	}
	if got := miniotest.Upload(t, up.URL, up.Headers, []byte("short")); got < 400 {
		t.Errorf("undersize body accepted: status %d", got)
	}
	// ... and the content type.
	if got := miniotest.Upload(t, up.URL, map[string]string{"Content-Type": "text/html"}, []byte(strings.Repeat("x", 10))); got < 400 {
		t.Errorf("different content type accepted: status %d", got)
	}
	if _, err := st.Stat(ctx, key); !errors.Is(err, course.ErrAssetNotFound) {
		t.Errorf("a rejected upload left an object behind: %v", err)
	}
	// The exact request still works.
	if got := miniotest.Upload(t, up.URL, up.Headers, []byte(strings.Repeat("x", 10))); got != 200 {
		t.Errorf("exact upload status = %d", got)
	}
}

func TestStore_PresignedURLExpires(t *testing.T) {
	st, _ := newStore(t)
	up, err := st.PresignUpload(context.Background(), key, "application/pdf", 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	if got := miniotest.Upload(t, up.URL, up.Headers, []byte("abc")); got < 400 {
		t.Fatalf("expired URL accepted: status %d", got)
	}
}

// A signed URL cannot be redirected to another key (another course's prefix,
// a traversal path): the key is part of the signature.
func TestStore_PresignedURLCannotBeRetargeted(t *testing.T) {
	st, _ := newStore(t)
	up, err := st.PresignUpload(context.Background(), key, "application/pdf", 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, other := range []string{
		strings.Replace(u.Path, "0190f0b0-0000-7000-8000-000000000001", "0190f0b0-0000-7000-8000-0000000000ff", 1),
		u.Path + "x",
		strings.Replace(u.Path, "/courses/", "/courses/../", 1),
	} {
		v := *u
		v.Path = other
		v.RawPath = ""
		if got := miniotest.Upload(t, v.String(), up.Headers, []byte("abc")); got < 400 {
			t.Errorf("retargeted URL %q accepted: status %d", other, got)
		}
	}
}

// URLs are signed for the PUBLIC endpoint (what a browser reaches), while Stat
// dials the internal one.
func TestStore_PresignUsesPublicEndpoint(t *testing.T) {
	env := miniotest.Setup(t)
	cfg := env.Config
	cfg.PublicEndpoint = "https://assets.example.test"
	st, err := assets.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	up, err := st.PresignUpload(context.Background(), key, "application/pdf", 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(up.URL, "https://assets.example.test/") {
		t.Fatalf("URL = %s, want the public endpoint", up.URL)
	}
	if _, err := st.Stat(context.Background(), key); !errors.Is(err, course.ErrAssetNotFound) {
		t.Fatalf("Stat via internal endpoint: %v", err)
	}
}
