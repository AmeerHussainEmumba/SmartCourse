//go:build integration

package e2e_test

// Correctness and contract scenarios E8 to E11 are implemented (build tag e2e,
// they need the container harness in test/e2e/harness and run in the e2e CI
// job): E8 and E9 in scenarios_correctness_e2e_test.go, E10 in
// scenarios_redis_e2e_test.go, E11 in scenarios_schema_e2e_test.go. Their
// specs live there, with the implementations.
