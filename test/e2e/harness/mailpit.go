package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// mailpitImage is the image docker-compose.yml pins for the SMTP sink.
const mailpitImage = "axllent/mailpit:v1.34.0"

// Mailpit is the SMTP sink the worker sends to, and the HTTP API the scenarios
// read mail from. Both ports are fixed on the host, so StopSMTP followed by
// StartSMTP brings the same address back (the worker's SMTP_HOST/SMTP_PORT
// never change). Mailpit keeps mail in memory: a restart empties the inbox.
type Mailpit struct {
	SMTPAddr string // host:port the worker connects to
	HTTPURL  string // base URL of the web UI and API

	smtpPort  int
	container testcontainers.Container
}

// Message is the part of a Mailpit message summary the scenarios use.
type Message struct {
	ID      string
	Subject string
	To      []string
	Created time.Time
}

func startMailpit(t testing.TB) *Mailpit {
	t.Helper()
	smtpPort, err := freePort()
	if err != nil {
		t.Fatalf("pick a port for mailpit smtp: %v", err)
	}
	httpPort, err := freePort()
	if err != nil {
		t.Fatalf("pick a port for mailpit http: %v", err)
	}
	c, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        mailpitImage,
			ExposedPorts: []string{"1025/tcp", "8025/tcp"},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = network.PortMap{
					network.MustParsePort("1025/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(smtpPort)}},
					network.MustParsePort("8025/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(httpPort)}},
				}
			},
			WaitingFor: wait.ForHTTP("/readyz").WithPort("8025/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start mailpit container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	return &Mailpit{
		SMTPAddr:  fmt.Sprintf("127.0.0.1:%d", smtpPort),
		HTTPURL:   fmt.Sprintf("http://127.0.0.1:%d", httpPort),
		smtpPort:  smtpPort,
		container: c,
	}
}

// StopSMTP stops Mailpit (the provider goes down: connections are refused).
func (m *Mailpit) StopSMTP(t testing.TB) {
	t.Helper()
	if err := m.container.Stop(context.Background(), nil); err != nil {
		t.Fatalf("stop mailpit: %v", err)
	}
}

// StartSMTP starts Mailpit again on the same ports and waits until it answers.
// The inbox is empty after a restart.
func (m *Mailpit) StartSMTP(t testing.TB) {
	t.Helper()
	if err := m.container.Start(context.Background()); err != nil {
		t.Fatalf("restart mailpit: %v", err)
	}
	Eventually(t, time.Minute, "mailpit to answer after restart", func() error {
		_, err := m.Messages()
		return err
	})
}

// Messages lists the messages in the inbox (up to 1000, newest first).
func (m *Mailpit) Messages() ([]Message, error) {
	var body struct {
		Messages []struct {
			ID      string `json:"ID"`
			Subject string `json:"Subject"`
			Created string `json:"Created"`
			To      []struct {
				Address string `json:"Address"`
			} `json:"To"`
		} `json:"messages"`
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(m.HTTPURL + "/api/v1/messages?limit=1000")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mailpit GET /api/v1/messages: HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode mailpit messages: %w", err)
	}
	out := make([]Message, 0, len(body.Messages))
	for _, mm := range body.Messages {
		msg := Message{ID: mm.ID, Subject: mm.Subject}
		msg.Created, _ = time.Parse(time.RFC3339Nano, mm.Created)
		for _, to := range mm.To {
			msg.To = append(msg.To, strings.ToLower(to.Address))
		}
		out = append(out, msg)
	}
	return out, nil
}

// MessagesTo lists the messages addressed to email.
func (m *Mailpit) MessagesTo(email string) ([]Message, error) {
	all, err := m.Messages()
	if err != nil {
		return nil, err
	}
	email = strings.ToLower(email)
	var out []Message
	for _, msg := range all {
		for _, to := range msg.To {
			if to == email {
				out = append(out, msg)
				break
			}
		}
	}
	return out, nil
}

// MessagesToWithSubject lists the messages to email whose subject contains
// part (case-insensitive): "enrol", "complet", "welcome".
func (m *Mailpit) MessagesToWithSubject(email, part string) ([]Message, error) {
	msgs, err := m.MessagesTo(email)
	if err != nil {
		return nil, err
	}
	var out []Message
	for _, msg := range msgs {
		if strings.Contains(strings.ToLower(msg.Subject), strings.ToLower(part)) {
			out = append(out, msg)
		}
	}
	return out, nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
