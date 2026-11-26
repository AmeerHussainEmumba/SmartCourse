// Command audit-api serves the failed-events admin surface (ADR-0031): list,
// inspect and replay parked events, admin role only, behind the fail-closed
// token-version check. Replay republishes to the consumer's retry topic. Port
// 8080 in the container (host 8093); /metrics and the probes are on the
// internal listener (METRICS_HTTP_ADDR), /metrics never on the API port.
package main

import (
	rt "github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

func main() {
	rt.Main("audit-api", wire, rt.WithConfig(loadConfig))
}
