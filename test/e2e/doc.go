// Package e2e holds tests that run more than one SmartCourse service together,
// against one shared Postgres and one shared Redis. The tests are in
// *_test.go files behind the `integration` build tag (they need Docker);
// this file exists so the package is not empty without that tag, which tools
// such as golangci-lint rely on.
//
// It is its own Go module, and one of the few allowed to depend on more than
// one service (scripts/check-module-graph.sh), because the point of a test
// here is to run the services side by side.
package e2e
