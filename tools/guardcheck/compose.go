package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// service is one compose service with its environment resolved from every
// form compose accepts: a map ("KEY: value"), a list ("- KEY=value", "- KEY"),
// YAML anchors and merge keys (yaml.v3 resolves them), and env_file.
type service struct {
	name    string
	env     map[string]string
	ports   []any
	netHost bool
	rawEnv  bool // an environment value could not be resolved to a literal
}

type composeFile struct {
	path     string
	services []service
}

func loadCompose(root string) (*composeFile, error) {
	path := filepath.Join(root, "docker-compose.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Services map[string]map[string]any `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cf := &composeFile{path: path}
	names := make([]string, 0, len(doc.Services))
	for n := range doc.Services {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		raw := doc.Services[n]
		s := service{name: n, env: map[string]string{}}
		for _, f := range envFiles(raw["env_file"]) {
			for k, v := range readDotenv(filepath.Join(root, f)) {
				s.env[k] = v
			}
		}
		switch e := raw["environment"].(type) {
		case map[string]any:
			for k, v := range e {
				if v == nil {
					s.env[k] = ""
				} else {
					s.env[k] = fmt.Sprint(v)
				}
			}
		case []any:
			for _, it := range e {
				str, isStr := it.(string)
				if !isStr {
					s.rawEnv = true
					continue
				}
				k, v, _ := strings.Cut(str, "=")
				s.env[strings.TrimSpace(k)] = v
			}
		case nil:
		default:
			s.rawEnv = true
		}
		if p, isList := raw["ports"].([]any); isList {
			s.ports = p
		}
		if nm, _ := raw["network_mode"].(string); nm == "host" {
			s.netHost = true
		}
		cf.services = append(cf.services, s)
	}
	return cf, nil
}

func envFiles(v any) []string {
	switch e := v.(type) {
	case string:
		return []string{e}
	case []any:
		var out []string
		for _, it := range e {
			switch x := it.(type) {
			case string:
				out = append(out, x)
			case map[string]any:
				if p, _ := x["path"].(string); p != "" {
					out = append(out, p)
				}
			}
		}
		return out
	}
	return nil
}

// readDotenv reads KEY=value lines; a missing file reads as empty (compose
// itself fails on it, so it cannot hide a value that would be used).
func readDotenv(path string) map[string]string {
	out := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return out
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		k, v, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		out[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
	}
	return out
}

const signingKeyVar = "JWT_SIGNING_KEY"

// checkA: every compose service that carries a JWT_SIGNING_KEY pins
// APP_ENV to the literal "development". A value that is not a literal
// (${APP_ENV:-production}, an empty value) cannot be shown to be development,
// so it fails too.
func checkA(cf *composeFile) Finding {
	var bad []string
	for _, s := range cf.services {
		if _, has := s.env[signingKeyVar]; !has {
			continue
		}
		if env, set := s.env["APP_ENV"]; !set || strings.TrimSpace(env) != "development" {
			bad = append(bad, s.name)
		}
	}
	if len(bad) > 0 {
		return fail("A", "compose service(s) with a %s but without APP_ENV: development: %s", signingKeyVar, strings.Join(bad, " "))
	}
	return ok("A", "every compose service with a signing key is pinned to APP_ENV=development")
}

// publishedKeys are the JWT_SIGNING_KEY values published in docker-compose.yml
// (check B needs them to prove the production rejection covers them).
func (cf *composeFile) publishedKeys() []string {
	var out []string
	for _, s := range cf.services {
		if v := strings.TrimSpace(s.env[signingKeyVar]); v != "" && !strings.Contains(v, "${") {
			out = append(out, v)
		}
	}
	return out
}

var credentialEnvMarkers = []string{"PASSWORD", "SECRET", "SIGNING_KEY", "ACCESS_KEY", "TOKEN", "CREDENTIAL"}

func (s service) hasDefaultCredentials() bool {
	for k, v := range s.env {
		if v == "" {
			continue
		}
		u := strings.ToUpper(k)
		for _, m := range credentialEnvMarkers {
			if strings.Contains(u, m) {
				return true
			}
		}
	}
	return false
}

// hostIP returns the interface a published port binds to ("" is every
// interface) and whether the entry publishes a port at all.
func hostIP(p any) (ip string, ok bool) {
	switch e := p.(type) {
	case int:
		return "", true // "5432": container port only, ephemeral host port on all interfaces
	case string:
		s := e
		if i := strings.IndexByte(s, '/'); i >= 0 { // 8080:80/tcp
			s = s[:i]
		}
		if strings.HasPrefix(s, "[") { // [::1]:8080:80
			end := strings.Index(s, "]")
			if end < 0 {
				return "?", true
			}
			return s[1:end], true
		}
		parts := strings.Split(s, ":")
		if len(parts) == 3 {
			return parts[0], true
		}
		return "", true
	case map[string]any:
		if hi, _ := e["host_ip"].(string); hi != "" {
			return hi, true
		}
		return "", true
	}
	return "?", true
}

func isLoopback(ip string) bool {
	if ip == "localhost" {
		return true
	}
	p := net.ParseIP(ip)
	return p != nil && p.IsLoopback()
}

// checkG: a published port must bind a loopback address. Short and long port
// syntax, inline and block lists, and published-container-only entries are all
// read from the parsed YAML. It is a FAIL when the service carries default
// credentials (the data store is then open to the LAN with a known password),
// a WARN otherwise (hardening gap).
func checkG(cf *composeFile) []Finding {
	var out []Finding
	var lines []string
	level := "WARN"
	for _, s := range cf.services {
		if s.netHost {
			lines = append(lines, fmt.Sprintf("%s: network_mode: host (every port on every interface)", s.name))
			if s.hasDefaultCredentials() {
				level = "FAIL"
			}
		}
		for _, p := range s.ports {
			ip, published := hostIP(p)
			if published && !isLoopback(ip) {
				bind := ip
				if bind == "" {
					bind = "all interfaces"
				}
				lines = append(lines, fmt.Sprintf("%s: %v binds %s", s.name, p, bind))
				if s.hasDefaultCredentials() {
					level = "FAIL"
				}
			}
		}
	}
	if len(lines) == 0 {
		return []Finding{ok("G", "every published compose port binds 127.0.0.1")}
	}
	msg := "docker-compose publishes ports beyond the loopback interface (reachable from the LAN); bind 127.0.0.1:\n" + indent(lines)
	if level == "FAIL" {
		msg = "docker-compose publishes ports beyond the loopback interface next to default credentials in the same service; bind 127.0.0.1:\n" + indent(lines)
	}
	out = append(out, Finding{level, "G", msg})
	return out
}
