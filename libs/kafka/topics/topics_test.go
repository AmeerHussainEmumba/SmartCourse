package topics

import (
	"os"
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
	m := regexp.MustCompile(`for t in ([a-z. ]+); do`).FindSubmatch(data)
	if m == nil {
		t.Fatal("kafka-init's `for t in ...; do` loop not found in docker-compose.yml; update this test with the file")
	}
	fromCompose := strings.Fields(string(m[1]))
	got := Names()
	slices.Sort(fromCompose)
	slices.Sort(got)
	if !slices.Equal(fromCompose, got) {
		t.Fatalf("compose creates %v, topics.All() has %v", fromCompose, got)
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
