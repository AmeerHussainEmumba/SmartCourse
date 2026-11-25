// Command guardcheck is the structured half of scripts/check-dev-defaults.sh
// (checks A, B, C, D and G). It parses docker-compose.yml as YAML and the Go
// sources with go/parser instead of grepping them, because grep cannot tell
// code from a comment, a double-quoted string from a raw string, a list from
// an inline list, or a map from a list of KEY=value (docs/plans/
// week-3-findings-security.md, S-12). It uses the standard library plus
// gopkg.in/yaml.v3 only, so it builds with the repo's own Go toolchain and
// needs no interpreter on a CI image.
//
// Output is one line per result, "ok   [X] ...", "WARN [X] ..." or
// "FAIL [X] ...", in the format of the shell script. The exit status is 1 when
// any line is a FAIL, and 2 when the tree cannot be read at all (a guard that
// cannot look must fail, never pass).
//
//	go run . -root <repo root>
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// Finding is one result line.
type Finding struct {
	Level string // "ok", "WARN" or "FAIL"
	Check string // "A", "B", "C", "D", "G"
	Msg   string
}

func (f Finding) String() string {
	if f.Level == "ok" {
		return fmt.Sprintf("ok   [%s] %s", f.Check, f.Msg)
	}
	return fmt.Sprintf("%s [%s] %s", f.Level, f.Check, f.Msg)
}

func ok(check, format string, a ...any) Finding {
	return Finding{"ok", check, fmt.Sprintf(format, a...)}
}
func fail(check, format string, a ...any) Finding {
	return Finding{"FAIL", check, fmt.Sprintf(format, a...)}
}

func main() {
	root := flag.String("root", ".", "repository root to check")
	flag.Parse()

	compose, err := loadCompose(*root)
	var all []Finding
	if err != nil {
		// Without the compose file neither A, B (published keys) nor G can
		// be evaluated: that is a failure, not a pass.
		all = append(all, fail("A", "cannot evaluate docker-compose.yml: %v", err))
		all = append(all, fail("G", "cannot evaluate docker-compose.yml: %v", err))
	} else {
		all = append(all, checkA(compose))
		all = append(all, checkG(compose)...)
	}
	all = append(all, checkB(*root, compose))
	all = append(all, checkC(*root)...)
	all = append(all, checkD(*root)...)

	failed := false
	for _, f := range all {
		fmt.Println(f)
		if f.Level == "FAIL" {
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

// indent prefixes every line of s for the multi-line detail the shell script
// prints under a finding.
func indent(lines []string) string {
	return "       " + strings.Join(lines, "\n       ")
}
