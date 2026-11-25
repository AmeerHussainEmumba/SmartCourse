// Package e2e holds the end-to-end tests of SmartCourse: the ten service
// binaries run side by side as processes on real containers (see harness and
// test/README.md). The tests are in *_test.go files behind the `e2e` build tag
// (they need Docker); this file exists so the package is not empty without
// that tag, which tools such as golangci-lint rely on.
//
// It is its own Go module and requires no service module
// (scripts/check-module-graph.sh): it builds the binaries and talks to them
// over HTTP, Kafka and the databases.
package e2e
