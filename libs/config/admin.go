package config

import (
	"fmt"
	"net"
	"strconv"
)

// AdminConfig is the worker's admin HTTP surface (services/worker/admin:
// failed-event listing and replay, health). Only the worker binary serves it,
// but every binary reads the same environment, so Load validates it for all.
type AdminConfig struct {
	// Addr is ADMIN_HTTP_ADDR, a host:port to listen on (default ":8093").
	// It is a privileged surface: bind it to a private interface or put it
	// behind the same network controls as the other admin routes.
	Addr string
}

const defaultAdminAddr = ":8093"

func loadAdmin(getenv func(string) string) (AdminConfig, []string) {
	a := AdminConfig{Addr: getenv("ADMIN_HTTP_ADDR")}
	if a.Addr == "" {
		a.Addr = defaultAdminAddr
	}
	host, port, err := net.SplitHostPort(a.Addr)
	if err != nil {
		return a, []string{fmt.Sprintf("ADMIN_HTTP_ADDR must be host:port, for example :8093 (%v)", err)}
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return a, []string{"ADMIN_HTTP_ADDR port must be a number from 1 to 65535"}
	}
	if host != "" && net.ParseIP(host) == nil && !validHostname(host) {
		return a, []string{"ADMIN_HTTP_ADDR host must be an IP address or a host name"}
	}
	return a, nil
}

func validHostname(h string) bool {
	if len(h) > 253 {
		return false
	}
	for _, r := range h {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
		default:
			return false
		}
	}
	return true
}
