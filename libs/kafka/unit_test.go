package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoff_DoublesThenCaps(t *testing.T) {
	b := BackoffConfig{Base: time.Second, Max: 10 * time.Second}
	want := []time.Duration{1, 2, 4, 8, 10, 10, 10}
	for n, w := range want {
		if got := b.Delay(n); got != w*time.Second {
			t.Errorf("Delay(%d) = %v, want %v", n, got, w*time.Second)
		}
	}
}

func TestBackoff_ZeroValueUsesDefaults(t *testing.T) {
	var b BackoffConfig
	if got := b.Delay(0); got != DefaultBackoff.Base {
		t.Errorf("Delay(0) = %v, want %v", got, DefaultBackoff.Base)
	}
	if got := b.Delay(100); got != DefaultBackoff.Max {
		t.Errorf("Delay(100) = %v, want %v", got, DefaultBackoff.Max)
	}
}

func TestBackoff_HugeRetryCountDoesNotOverflow(t *testing.T) {
	b := BackoffConfig{Base: time.Second, Max: time.Minute}
	if got := b.Delay(1 << 30); got != time.Minute {
		t.Fatalf("Delay(huge) = %v, want the cap", got)
	}
}

func TestBackoff_MaxBelowBaseIsRaisedToBase(t *testing.T) {
	b := BackoffConfig{Base: 5 * time.Second, Max: time.Second}
	if got := b.Delay(3); got != 5*time.Second {
		t.Fatalf("Delay(3) = %v, want Base when Max < Base", got)
	}
}

func TestBackoff_JitterOnlyShortens(t *testing.T) {
	b := BackoffConfig{Base: time.Second, Max: 8 * time.Second, Jitter: 0.5}
	for range 200 {
		d := b.Delay(10)
		if d > 8*time.Second || d < 4*time.Second {
			t.Fatalf("jittered capped delay %v outside [4s, 8s]", d)
		}
	}
}

func TestBackoff_Validate(t *testing.T) {
	for name, b := range map[string]BackoffConfig{
		"negative base":    {Base: -1},
		"negative max":     {Max: -1},
		"negative jitter":  {Jitter: -0.1},
		"jitter of one":    {Jitter: 1},
		"jitter above one": {Jitter: 2},
	} {
		if err := b.validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: validate = %v, want ErrInvalidConfig", name, err)
		}
	}
	if err := (BackoffConfig{}).validate(); err != nil {
		t.Errorf("zero value must be valid: %v", err)
	}
}

func okHandler(context.Context, Record) error { return nil }

func TestProducerConfig_Validate(t *testing.T) {
	ok := ProducerConfig{Brokers: []string{"localhost:9092"}}
	if _, err := NewProducer(ok); err != nil {
		t.Fatalf("a minimal config must be valid: %v", err)
	}
	for name, mutate := range map[string]func(*ProducerConfig){
		"no brokers":       func(c *ProducerConfig) { c.Brokers = nil },
		"empty broker":     func(c *ProducerConfig) { c.Brokers = []string{""} },
		"negative retries": func(c *ProducerConfig) { c.MaxRetries = -1 },
		"negative timeout": func(c *ProducerConfig) { c.DeliveryTimeout = -1 },
		"bad backoff":      func(c *ProducerConfig) { c.Backoff.Jitter = 1.5 },
	} {
		c := ok
		mutate(&c)
		if _, err := NewProducer(c); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: NewProducer = %v, want ErrInvalidConfig", name, err)
		}
	}
}

func TestConsumerConfig_Validate(t *testing.T) {
	ok := ConsumerConfig{
		Brokers: []string{"localhost:9092"}, GroupID: "g", Topics: []string{"t"}, Handler: okHandler,
	}
	c, err := NewConsumer(ok)
	if err != nil {
		t.Fatalf("a minimal config must be valid: %v", err)
	}
	if c.cfg.MaxInFlight != DefaultMaxInFlight || c.cfg.MaxAttempts != DefaultMaxAttempts || c.cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Errorf("defaults not applied: %+v", c.cfg)
	}

	for name, mutate := range map[string]func(*ConsumerConfig){
		"no brokers":           func(c *ConsumerConfig) { c.Brokers = nil },
		"no group":             func(c *ConsumerConfig) { c.GroupID = "" },
		"no topics":            func(c *ConsumerConfig) { c.Topics = nil },
		"empty topic":          func(c *ConsumerConfig) { c.Topics = []string{""} },
		"no handler":           func(c *ConsumerConfig) { c.Handler = nil },
		"negative in-flight":   func(c *ConsumerConfig) { c.MaxInFlight = -1 },
		"negative attempts":    func(c *ConsumerConfig) { c.MaxAttempts = -1 },
		"negative shutdown":    func(c *ConsumerConfig) { c.ShutdownTimeout = -1 },
		"negative session":     func(c *ConsumerConfig) { c.SessionTimeout = -1 },
		"bad backoff (jitter)": func(c *ConsumerConfig) { c.Backoff.Jitter = -1 },
	} {
		c := ok
		mutate(&c)
		if _, err := NewConsumer(c); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: NewConsumer = %v, want ErrInvalidConfig", name, err)
		}
	}
}

func TestConsumer_RunTwiceIsRejected(t *testing.T) {
	c, err := NewConsumer(ConsumerConfig{
		Brokers: []string{"127.0.0.1:1"}, GroupID: "g", Topics: []string{"t"}, Handler: okHandler,
		ShutdownTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // returns at once: nothing to drain, no broker needed
	if err := c.Run(ctx); err != nil {
		t.Fatalf("first Run on a cancelled context = %v, want nil", err)
	}
	if err := c.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run = %v, want ErrAlreadyRunning", err)
	}
}

func TestProduce_RejectsMissingTopic(t *testing.T) {
	p, err := NewProducer(ProducerConfig{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Produce(context.Background(), Message{Value: []byte("x")}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Produce without a topic = %v, want ErrInvalidConfig", err)
	}
}

func TestProduce_AfterCloseIsErrClosed(t *testing.T) {
	p, err := NewProducer(ProducerConfig{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Close() // twice is fine
	if _, err := p.Produce(context.Background(), Message{Topic: "t"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Produce after Close = %v, want ErrClosed", err)
	}
}

func TestRecord_Header(t *testing.T) {
	r := Record{Headers: []Header{{Key: "a", Value: []byte("1")}, {Key: "a", Value: []byte("2")}}}
	if v, ok := r.Header("a"); !ok || string(v) != "1" {
		t.Errorf("Header(a) = %q, %v; want the first", v, ok)
	}
	if _, ok := r.Header("b"); ok {
		t.Error("Header(b) found a header that is not there")
	}
}

func TestExhaustedError_ReadsAsLastAndCarriesEveryAttempt(t *testing.T) {
	first, last := errors.New("first"), errors.New("last")
	err := error(&ExhaustedError{Attempts: []error{first, last}})
	if err.Error() != "last" || !errors.Is(err, last) || errors.Is(err, first) {
		t.Fatalf("must read and unwrap as the last attempt: %v", err)
	}
	if got := AttemptErrors(err); len(got) != 2 || got[0] != first || got[1] != last {
		t.Fatalf("AttemptErrors = %v", got)
	}
	// An error that carries no history is its own one-entry history.
	plain := errors.New("plain")
	if got := AttemptErrors(plain); len(got) != 1 || got[0] != plain {
		t.Fatalf("AttemptErrors(plain) = %v", got)
	}
	if got := AttemptErrors(nil); got != nil {
		t.Fatalf("AttemptErrors(nil) = %v", got)
	}
}
