package telemetry

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// boundedLabels are the only label names a SmartCourse metric may carry, with
// what bounds each one (ADR-0032: route templates, never raw identifiers):
//
//	route    registered route templates plus the constant "unmatched"
//	method   the standard HTTP methods plus "OTHER"
//	status   a 3-digit HTTP status code
//	queue    the three Asynq queues
//	group    Kafka consumer groups, one per registered handler
//	topic    the provisioned Kafka topics
//	consumer the registered consumer handlers
//	outcome  the five Outcome values
//
// group, topic and consumer are bounded by configuration (a fixed list in
// code), not by a function here: callers must pass those names, never
// anything derived from a request or an event.
var boundedLabels = map[string]bool{
	"route": true, "method": true, "status": true, "queue": true,
	"group": true, "topic": true, "consumer": true, "outcome": true,
}

// CheckLabels returns an error naming every metric in g that has a label
// outside the bounded set above. It is the cardinality guard: a metric
// labelled by user, course, event, request or trace id (or a raw path or URL)
// fails it. Run it against each service's registry in tests.
func CheckLabels(g prometheus.Gatherer) error {
	families, err := g.Gather()
	if err != nil {
		return fmt.Errorf("gather: %w", err)
	}
	var errs []error
	for _, f := range families {
		if isRuntimeMetric(f.GetName()) {
			continue
		}
		bad := map[string]bool{}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if !boundedLabels[l.GetName()] {
					bad[l.GetName()] = true
				}
			}
		}
		if len(bad) == 0 {
			continue
		}
		names := make([]string, 0, len(bad))
		for n := range bad {
			names = append(names, n)
		}
		sort.Strings(names)
		errs = append(errs, fmt.Errorf("metric %s has label(s) %v outside the bounded set (route, method, status, queue, group, topic, consumer, outcome)", f.GetName(), names))
	}
	return errors.Join(errs...)
}

// isRuntimeMetric is true for the Go runtime and process collectors, whose
// names and labels Prometheus defines (go_info carries a "version" label).
func isRuntimeMetric(name string) bool {
	return strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_")
}
