//go:build integration

package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

// mailpitImage is read from docker-compose.yml so the test runs the version
// the stack runs.
func mailpitImage(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	m := regexp.MustCompile(`image:\s*(axllent/mailpit:\S+)`).FindSubmatch(b)
	if m == nil {
		t.Fatal("no axllent/mailpit image in docker-compose.yml")
	}
	return string(m[1])
}

// TestMailpit_RealContainer_ReceivesRenderedMail sends through the real SMTP
// provider to the real Mailpit container and reads the result back through
// Mailpit's HTTP API.
func TestMailpit_RealContainer_ReceivesRenderedMail(t *testing.T) {
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        mailpitImage(t),
			ExposedPorts: []string{"1025/tcp", "8025/tcp"},
			WaitingFor:   wait.ForHTTP("/readyz").WithPort("8025/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start mailpit: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	smtpPort, err := ctr.MappedPort(ctx, "1025/tcp")
	if err != nil {
		t.Fatal(err)
	}
	apiPort, err := ctr.MappedPort(ctx, "8025/tcp")
	if err != nil {
		t.Fatal(err)
	}

	prov, err := NewSMTPProvider(config.SMTPConfig{
		Host: host, Port: int(smtpPort.Num()), From: "no-reply@smartcourse.local",
		ConnectTimeout: 5 * time.Second, SendTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	subject, text, html, err := Render(KindEnrollmentConfirm, TemplateData{
		Name: "Ada <b>Lovelace</b>\r\nBcc: victim@evil.test", CourseTitle: "Go 101"})
	if err != nil {
		t.Fatal(err)
	}
	if err := prov.Send(ctx, Message{
		ID: "11111111-2222-3333-4444-555555555555", To: "ada@example.com", ToName: "Ada\r\nBcc: victim@evil.test",
		Subject: subject, Text: text, HTML: html,
	}); err != nil {
		t.Fatalf("send to mailpit: %v", err)
	}

	base := "http://" + host + ":" + strconv.Itoa(int(apiPort.Num()))
	var list struct {
		Total    int `json:"total"`
		Messages []struct {
			ID      string `json:"ID"`
			Subject string `json:"Subject"`
			To      []struct{ Address string }
			Bcc     []struct{ Address string }
		} `json:"messages"`
	}
	getJSON(t, base+"/api/v1/messages", &list)
	if list.Total != 1 || len(list.Messages) != 1 {
		t.Fatalf("want exactly 1 message in Mailpit, got %d", list.Total)
	}
	m := list.Messages[0]
	if m.Subject != "You are enrolled in Go 101" {
		t.Fatalf("subject %q", m.Subject)
	}
	if len(m.To) != 1 || m.To[0].Address != "ada@example.com" || len(m.Bcc) != 0 {
		t.Fatalf("recipients widened: to=%v bcc=%v", m.To, m.Bcc)
	}

	var full struct {
		Text string
		HTML string
	}
	getJSON(t, base+"/api/v1/message/"+m.ID, &full)
	// The display name is sanitized (S-10): markup characters are dropped, so no
	// tag survives in either body, escaped or not, and the readable part stays.
	if !strings.Contains(full.Text, "Go 101") || !strings.Contains(full.HTML, "Lovelace") ||
		strings.Contains(full.HTML, "<b>") || strings.Contains(full.HTML, "&lt;b&gt;") {
		t.Fatalf("bodies not as rendered:\ntext=%q\nhtml=%q", full.Text, full.HTML)
	}
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %s", url, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(fmt.Errorf("decode %s: %w", url, err))
	}
}
