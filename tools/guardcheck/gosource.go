package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func parseGo(fset *token.FileSet, path string) (*ast.File, error) {
	return parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
}

// goFiles lists the non-test .go files under dir, skipping vendored and
// generated-by-tooling directories.
func goFiles(dir string, skip func(rel string, d fs.DirEntry) bool) ([]string, error) {
	var out []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			if skip != nil && skip(rel, d) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
			out = append(out, p)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

// constString evaluates an expression made only of string literals (quoted or
// raw), "+" concatenation, parentheses and string/[]byte conversions.
func constString(e ast.Expr) (string, bool) {
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(x.Value)
		return s, err == nil
	case *ast.ParenExpr:
		return constString(x.X)
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return "", false
		}
		l, lok := constString(x.X)
		r, rok := constString(x.Y)
		return l + r, lok && rok
	case *ast.CallExpr:
		if len(x.Args) != 1 {
			return "", false
		}
		switch f := x.Fun.(type) {
		case *ast.Ident:
			if f.Name == "string" {
				return constString(x.Args[0])
			}
		case *ast.ArrayType:
			return constString(x.Args[0]) // []byte("...")
		}
	}
	return "", false
}

// nameOf is the identifier a value is assigned to: x, s.x, m["x"].
func nameOf(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	case *ast.IndexExpr:
		if s, ok := constString(x.Index); ok {
			return s
		}
	case *ast.BasicLit:
		if s, ok := constString(x); ok {
			return s
		}
	}
	return ""
}

var credentialNames = []string{
	"password", "passwd", "secret", "signingkey", "signing_key", "apikey", "api_key",
	"accesskey", "access_key", "privatekey", "private_key", "token", "credential",
}

func credentialLike(name string) bool {
	l := strings.ToLower(name)
	for _, c := range credentialNames {
		if strings.Contains(l, c) {
			return true
		}
	}
	return false
}

const minCredentialLen = 6

// envVarName matches a SCREAMING_SNAKE value ("JWT_SIGNING_KEY").
var envVarName = regexp.MustCompile(`^[A-Z][A-Z0-9]*(_[A-Z0-9]+)+$`)

// namesAnEnvVar is true for a constant that holds the NAME of an environment
// variable, not a secret: code that reads its configuration names such
// variables in constants whose own name contains "key" or "secret"
// (signingKeyVar = "JWT_SIGNING_KEY"). Both the value's shape and the
// constant's name (…Var, …Env, …Name) must say so, so an upper-case secret
// under a plain name is still reported.
func namesAnEnvVar(name, val string) bool {
	l := strings.ToLower(name)
	return envVarName.MatchString(val) &&
		(strings.HasSuffix(l, "var") || strings.HasSuffix(l, "env") || strings.HasSuffix(l, "name"))
}

// checkC: no credential-looking literal in production Go code. Found from the
// AST, so a raw (backtick) string, a "+" concatenation, a []byte conversion, a
// struct field or a map entry is seen like a plain "x" := "y". tools/seed (a
// development tool, see check D), test files, testdata and the test/ tree
// (test harness code, never linked into a binary) are not scanned.
func checkC(root string) []Finding {
	files, err := goFiles(root, func(rel string, d fs.DirEntry) bool {
		r := filepath.ToSlash(rel)
		if r == "test" || r == "tools/seed" {
			return true
		}
		// Test-support packages (testenv, miniotest, ...): linked only into
		// tests, they carry throwaway container credentials by design.
		base := d.Name()
		return strings.HasPrefix(base, "test") || strings.HasSuffix(base, "test")
	})
	if err != nil {
		return []Finding{fail("C", "cannot scan Go sources: %v", err)}
	}
	fset := token.NewFileSet()
	var hits []string
	for _, f := range files {
		file, err := parseGo(fset, f)
		if err != nil {
			rel, _ := filepath.Rel(root, f)
			hits = append(hits, fmt.Sprintf("%s: cannot be parsed, so it cannot be checked (%v)", filepath.ToSlash(rel), err))
			continue
		}
		report := func(name string, pos token.Pos, val string) {
			if credentialLike(name) && len(val) >= minCredentialLen && !namesAnEnvVar(name, val) {
				p := fset.Position(pos)
				rel, _ := filepath.Rel(root, p.Filename)
				hits = append(hits, fmt.Sprintf("./%s:%d: %s = <%d-char literal>", filepath.ToSlash(rel), p.Line, name, len(val)))
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for i, l := range x.Lhs {
					if i < len(x.Rhs) {
						if v, ok := constString(x.Rhs[i]); ok {
							report(nameOf(l), x.Rhs[i].Pos(), v)
						}
					}
				}
			case *ast.ValueSpec:
				for i, id := range x.Names {
					if i < len(x.Values) {
						if v, ok := constString(x.Values[i]); ok {
							report(id.Name, x.Values[i].Pos(), v)
						}
					}
				}
			case *ast.KeyValueExpr:
				if v, ok := constString(x.Value); ok {
					report(nameOf(x.Key), x.Value.Pos(), v)
				}
			}
			return true
		})
	}
	if len(hits) > 0 {
		sort.Strings(hits)
		return []Finding{fail("C", "credential-looking literals in production Go code:\n%s", indent(hits))}
	}
	return []Finding{ok("C", "no credential literals in non-test Go code outside tools/seed and test/")}
}

// ---- B ---------------------------------------------------------------------

// pkgInfo is what check B needs about libs/config: the string literals in
// each package-level var and function (comments are not part of the AST, so
// they contribute nothing) and the functions an Env-conditional statement calls.
type pkgInfo struct {
	varLits  map[string][]string // package-level var/const -> literals in its initializer
	funcLits map[string][]string // func name -> literals it contains
	funcRefs map[string][]string // func name -> identifiers it mentions
	guarded  map[string]bool     // functions called from inside an `if` that tests Env
}

func collectLits(n ast.Node) []string {
	var out []string
	ast.Inspect(n, func(m ast.Node) bool {
		if bl, ok := m.(*ast.BasicLit); ok && bl.Kind == token.STRING {
			if s, err := strconv.Unquote(bl.Value); err == nil {
				out = append(out, strings.ToLower(s))
			}
		}
		return true
	})
	return out
}

func mentionsEnv(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			if x.Sel.Name == "Env" {
				found = true
			}
		case *ast.Ident:
			if x.Name == "Env" {
				found = true
			}
		case *ast.BasicLit:
			if x.Kind == token.STRING && (x.Value == `"production"` || x.Value == `"development"` || x.Value == `"APP_ENV"`) {
				found = true
			}
		}
		return true
	})
	return found
}

func calledNames(n ast.Node) []string {
	var out []string
	ast.Inspect(n, func(m ast.Node) bool {
		if c, ok := m.(*ast.CallExpr); ok {
			switch f := c.Fun.(type) {
			case *ast.Ident:
				out = append(out, f.Name)
			case *ast.SelectorExpr:
				out = append(out, f.Sel.Name)
			}
		}
		return true
	})
	return out
}

func loadPkg(dir string) (*pkgInfo, error) {
	files, err := goFiles(dir, nil)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no Go files in %s", dir)
	}
	info := &pkgInfo{varLits: map[string][]string{}, funcLits: map[string][]string{}, funcRefs: map[string][]string{}, guarded: map[string]bool{}}
	fset := token.NewFileSet()
	var parsed []*ast.File
	for _, f := range files {
		file, err := parseGo(fset, f)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", f, err)
		}
		parsed = append(parsed, file)
	}
	for _, file := range parsed {
		for _, d := range file.Decls {
			switch x := d.(type) {
			case *ast.GenDecl:
				for _, sp := range x.Specs {
					if vs, ok := sp.(*ast.ValueSpec); ok {
						for i, id := range vs.Names {
							if i < len(vs.Values) {
								info.varLits[id.Name] = append(info.varLits[id.Name], collectLits(vs.Values[i])...)
							}
						}
					}
				}
			case *ast.FuncDecl:
				if x.Body == nil {
					continue
				}
				info.funcLits[x.Name.Name] = append(info.funcLits[x.Name.Name], collectLits(x.Body)...)
				ast.Inspect(x.Body, func(n ast.Node) bool {
					switch y := n.(type) {
					case *ast.Ident:
						info.funcRefs[x.Name.Name] = append(info.funcRefs[x.Name.Name], y.Name)
					case *ast.BinaryExpr:
						// `cfg.Env != "development" && weak(key)`: the call is
						// conditional on the environment too.
						if (y.Op == token.LAND || y.Op == token.LOR) && (mentionsEnv(y.X) || mentionsEnv(y.Y)) {
							for _, c := range calledNames(y) {
								info.guarded[c] = true
							}
						}
					case *ast.IfStmt:
						if mentionsEnv(y.Cond) {
							for _, c := range calledNames(y.Body) {
								info.guarded[c] = true
							}
							if y.Else != nil {
								for _, c := range calledNames(y.Else) {
									info.guarded[c] = true
								}
							}
						}
					}
					return true
				})
			}
		}
	}
	return info, nil
}

// guardedLiterals is every literal reachable from a function that an
// Env-conditional `if` calls: the function's own literals and those of the
// package-level vars it mentions.
func (p *pkgInfo) guardedLiterals() []string {
	var out []string
	for fn := range p.guarded {
		out = append(out, p.funcLits[fn]...)
		for _, ref := range p.funcRefs[fn] {
			out = append(out, p.varLits[ref]...)
		}
	}
	return out
}

// envExampleKeys reads JWT_SIGNING_KEY=value lines (not comments) of env.example.
func envExampleKeys(root string) []string {
	f, err := os.Open(filepath.Join(root, "env.example"))
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if rest, ok := strings.CutPrefix(line, signingKeyVar+"="); ok {
			if v := strings.Trim(strings.TrimSpace(rest), `"'`); v != "" {
				out = append(out, v)
			}
		}
	}
	return out
}

// checkB: libs/config must reject the published placeholder signing keys when
// APP_ENV is not development. The proof is structural and read from the AST:
// an `if` that tests Env calls a function whose code (or the package-level
// list it mentions) holds a literal that occurs in every published key
// (env.example and docker-compose.yml). A comment mentioning "production" and
// "placeholder", an unused list, or a length-only check satisfies none of it.
// It does not prove the function rejects; the unit tests of libs/config do.
func checkB(root string, cf *composeFile) Finding {
	published := envExampleKeys(root)
	if cf != nil {
		published = append(published, cf.publishedKeys()...)
	}
	pkg, err := loadPkg(filepath.Join(root, "libs", "config"))
	if err != nil {
		return fail("B", "cannot read libs/config: %v", err)
	}
	lits := pkg.guardedLiterals()
	var uncovered []string
	for _, key := range published {
		lk := strings.ToLower(key)
		covered := false
		for _, l := range lits {
			if len(l) >= 6 && strings.Contains(lk, l) {
				covered = true
				break
			}
		}
		if !covered {
			uncovered = append(uncovered, key)
		}
	}
	if len(pkg.guarded) == 0 || len(lits) == 0 || len(uncovered) > 0 {
		msg := "libs/config does not reject the published placeholder signing keys when APP_ENV is not development: no Env-conditional code in libs/config calls a check whose literals match them (comments and unused lists do not count). Not rejected:"
		if len(uncovered) == 0 {
			uncovered = append(uncovered, "(no published key could be matched; no guarded check found)")
		}
		sort.Strings(uncovered)
		return fail("B", "%s\n%s\n       reproducer: go test -tags='integration bugrepro' -run TestBug_Load_ProductionAcceptsPublishedDefaultSigningKey ./libs/config", msg, indent(uncovered))
	}
	return ok("B", "libs/config rejects the published placeholder signing keys outside development (an Env-conditional check holds their markers)")
}

// ---- D ---------------------------------------------------------------------

// checkD: tools/seed creates accounts (an admin among them) with a fixed
// password, so its main must refuse to run unless the environment is
// development: an `if` in main that tests Env (cfg.Env, os.Getenv("APP_ENV"))
// and whose body exits, panics or returns. A comment is not an `if`.
func checkD(root string) []Finding {
	dir := filepath.Join(root, "tools", "seed")
	files, err := goFiles(dir, nil)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return []Finding{fail("D", "cannot read tools/seed: %v", err)}
	}
	fset := token.NewFileSet()
	var fixedPassword, guarded bool
	var pwLoc string
	for _, f := range files {
		file, err := parseGo(fset, f)
		if err != nil {
			return []Finding{fail("D", "cannot parse %s: %v", f, err)}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if vs, isVS := n.(*ast.ValueSpec); isVS {
				for i, id := range vs.Names {
					if i < len(vs.Values) && credentialLike(id.Name) {
						if v, isStr := constString(vs.Values[i]); isStr && len(v) >= minCredentialLen {
							fixedPassword = true
							p := fset.Position(id.Pos())
							pwLoc = fmt.Sprintf("%s:%d %s", filepath.Base(p.Filename), p.Line, id.Name)
						}
					}
				}
			}
			return true
		})
		for _, d := range file.Decls {
			fd, isFn := d.(*ast.FuncDecl)
			if !isFn || fd.Name.Name != "main" || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if is, isIf := n.(*ast.IfStmt); isIf && mentionsEnv(is.Cond) && refuses(is.Body) {
					guarded = true
				}
				return true
			})
		}
	}
	switch {
	case !fixedPassword:
		return []Finding{ok("D", "tools/seed declares no fixed credential literal")}
	case guarded:
		return []Finding{ok("D", "tools/seed main refuses to run outside the development environment")}
	default:
		return []Finding{fail("D", "tools/seed has a fixed password for every seeded account incl. an admin (%s) and its main has no `if` on the environment that exits: pointing POSTGRES_DSN at a real database seeds admin@smartcourse.dev with a public password (a comment mentioning APP_ENV does not count)", pwLoc)}
	}
}

// refuses reports whether a block exits the process, panics or returns.
func refuses(b *ast.BlockStmt) bool {
	found := false
	ast.Inspect(b, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ReturnStmt:
			found = true
		case *ast.CallExpr:
			switch f := x.Fun.(type) {
			case *ast.Ident:
				if f.Name == "panic" {
					found = true
				}
			case *ast.SelectorExpr:
				if strings.HasPrefix(f.Sel.Name, "Fatal") || (f.Sel.Name == "Exit" && nameOf(f.X) == "os") {
					found = true
				}
			}
		}
		return true
	})
	return found
}
