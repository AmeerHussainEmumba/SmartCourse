// Package integration holds the audit service's tests that need real Kafka and
// MongoDB together (build tag integration): the archiver end to end, and replay
// by republish against a real consumer group. It has no non-test code.
package integration
