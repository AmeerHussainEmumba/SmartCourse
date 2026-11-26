// Package kafka is the pure-Go Kafka transport for SmartCourse (ADR-0030,
// RFC §6.2, §6.5, §6.6, §6.9), built on franz-go. Nothing imports it yet; it is
// the layer the relay and the consumers will sit on.
//
//   - Producer: a synchronous produce (key, headers, context) that returns
//     after the broker acknowledges, with the idempotent producer, acks from
//     all in-sync replicas, and bounded retries with backoff.
//   - Consumer: a consumer-group runner. Records of one partition are handled
//     in order, at most MaxInFlight handlers run at once across partitions, an
//     offset is committed only after its handler returned nil (at-least-once),
//     a record that keeps failing is handed to OnExhausted and then skipped (unless
//     the hook reports it could not deal with it) so
//     a poison record never stalls its partition (RFC §6.9), and Run drains
//     in-flight work on cancellation within a deadline.
//   - Ping: a broker reachability check shaped for a non-fatal readiness probe.
//   - topics: the RFC §6.5 topic list as code, with an idempotent Ensure.
//
// The package knows nothing about events, envelopes or the schema registry:
// it moves bytes. It depends on no other SmartCourse module (testenv only in
// tests), and not on libs/telemetry: callers wire metrics through Stats and
// the hooks.
package kafka
