package topics

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestAll_IsTheRFCList(t *testing.T) {
	want := map[string]string{ // RFC §6.5: topic -> key
		"identity.events":   "user_id",
		"course.events":     "course_id",
		"enrollment.events": "course_id",
		"progress.events":   "enrollment_id",
	}
	all := All()
	if len(all) != len(want) {
		t.Fatalf("All() has %d topics, RFC §6.5 lists %d", len(all), len(want))
	}
	for _, tp := range all {
		if key, ok := want[tp.Name]; !ok || tp.Key != key {
			t.Errorf("topic %q key %q, want %q (listed: %v)", tp.Name, tp.Key, key, ok)
		}
		if tp.Partitions < 0 {
			t.Errorf("topic %q has negative partitions", tp.Name)
		}
	}
}

// TestAll_MatchesComposeKafkaInit fails when docker-compose's kafka-init
// creates a different set of topics than this package, so the two provisioning
// paths cannot drift apart silently.
func TestAll_MatchesComposeKafkaInit(t *testing.T) {
	data, err := os.ReadFile("../../../docker-compose.yml")
	if err != nil {
		t.Skipf("docker-compose.yml not reachable from this module: %v", err)
	}
	m := regexp.MustCompile(`for t in ([a-z.\- ]+); do`).FindSubmatch(data)
	if m == nil {
		t.Fatal("kafka-init's `for t in ...; do` loop not found in docker-compose.yml; update this test with the file")
	}
	fromCompose := strings.Fields(string(m[1]))
	got := ProvisionedNames()
	slices.Sort(fromCompose)
	slices.Sort(got)
	if !slices.Equal(fromCompose, got) {
		t.Fatalf("compose creates %v, topics.Provisioned() has %v", fromCompose, got)
	}
}

func TestOptions_Defaults(t *testing.T) {
	o := Options{}.withDefaults()
	if o.Partitions != DefaultPartitions || o.ReplicationFactor != DefaultReplicationFactor {
		t.Fatalf("defaults = %+v", o)
	}
	o = Options{Partitions: 6, ReplicationFactor: 3}.withDefaults()
	if o.Partitions != 6 || o.ReplicationFactor != 3 {
		t.Fatalf("explicit values overwritten: %+v", o)
	}
}

func TestEnsure_RejectsBadInput(t *testing.T) {
	if err := Ensure(t.Context(), nil); err == nil {
		t.Error("Ensure with no brokers succeeded")
	}
	if err := EnsureWith(t.Context(), []string{"x:1"}, Options{Partitions: -1}); err == nil {
		t.Error("EnsureWith with negative partitions succeeded")
	}
}

func TestRetryTopics_OnePerKnownConsumer(t *testing.T) {
	want := []string{
		"retry.certificate-issuer", "retry.analytics-aggregator", "retry.course-instructor-mirror",
		"retry.enrollment-course-archival", "retry.notification-dispatcher", "retry.audit-archiver",
	}
	var got []string
	for _, tp := range RetryTopics() {
		got = append(got, tp.Name)
		if !IsRetry(tp.Name) {
			t.Errorf("%s is not recognised as a retry topic", tp.Name)
		}
		if tp.Partitions != 0 {
			t.Errorf("%s has its own partition count %d; the default (3) applies to every topic", tp.Name, tp.Partitions)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("RetryTopics = %v, want %v", got, want)
	}
	if Retry("x") != "retry.x" {
		t.Errorf("Retry(x) = %q", Retry("x"))
	}
	for _, n := range Names() {
		if IsRetry(n) {
			t.Errorf("event topic %s looks like a retry topic", n)
		}
	}
}

// Every consumer name in the code base has a retry topic: read the handlers'
// consumer names and compare.
func TestRetryTopics_CoverEveryConsumerNameInTheCode(t *testing.T) {
	roots := []string{"../../../services"}
	re := regexp.MustCompile(`(?m)^const (?:consumerName|Consumer) += "([a-z-]+)"`)
	found := map[string]bool{}
	for _, root := range roots {
		_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			b, _ := os.ReadFile(p)
			for _, m := range re.FindAllStringSubmatch(string(b), -1) {
				found[m[1]] = true
			}
			return nil
		})
	}
	if len(found) == 0 {
		t.Skip("services/ not reachable from this module")
	}
	for name := range found {
		if !slices.Contains(Consumers(), name) {
			t.Errorf("consumer %q has no retry topic: add it to topics.Consumers (and to kafka-init)", name)
		}
	}
	for _, name := range Consumers() {
		if !found[name] {
			t.Errorf("topics.Consumers lists %q but no handler declares it", name)
		}
	}
}
