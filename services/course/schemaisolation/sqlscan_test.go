package schemaisolation_test

// Static half of the ADR-0024 isolation proof: parse every production Go file
// of this service, pull out the string literals that look like SQL, and fail if
// any of them (a) schema-qualifies a table, or (b) names a table owned by a
// schema that is NOT on this service's search_path. Table ownership is read
// from the migrations themselves, so adding a table to another schema
// automatically extends the forbidden set.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	ownSchema = "course"
	// schemas whose tables this service may reference by bare name
	// (POSTGRES_SEARCH_PATH in the Makefile run target and docker-compose).
	allowedSchemas = "course,platform,public"
)

var createTable = regexp.MustCompile(`(?im)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_][a-z0-9_]*)`)

func repoRoot(t *testing.T) string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	// services/<svc>/schemaisolation/sqlscan_test.go -> repo root
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
}

func tablesBySchema(t *testing.T) map[string][]string {
	root := filepath.Join(repoRoot(t), "migrations")
	out := map[string][]string{}
	for _, schema := range []string{"identity", "course", "analytics", "platform"} {
		files, _ := filepath.Glob(filepath.Join(root, schema, "*.up.sql"))
		if len(files) == 0 {
			t.Fatalf("no migrations found for %s under %s", schema, root)
		}
		for _, f := range files {
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range createTable.FindAllStringSubmatch(string(b), -1) {
				out[schema] = append(out[schema], m[1])
			}
		}
	}
	return out
}

var queryCalls = map[string]bool{"Table": true, "Joins": true, "Where": true, "Select": true, "Raw": true, "Exec": true, "Order": true, "Group": true, "Having": true}

func sqlLiterals(t *testing.T, serviceRoot string) map[string][]string {
	lits := map[string][]string{} // file -> literals
	fset := token.NewFileSet()
	err := filepath.Walk(serviceRoot, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "schemaisolation" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			// Arguments of query-building calls count as SQL even when they
			// are fragments (GORM Where/Joins/Table/Select), not full statements.
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && queryCalls[sel.Sel.Name] {
					for _, a := range call.Args {
						if bl, ok := a.(*ast.BasicLit); ok && bl.Kind == token.STRING {
							if s, err := strconv.Unquote(bl.Value); err == nil {
								lits[p] = append(lits[p], s)
							}
						}
					}
				}
			}
			bl, ok := n.(*ast.BasicLit)
			if !ok || bl.Kind != token.STRING {
				return true
			}
			s, err := strconv.Unquote(bl.Value)
			if err != nil {
				return true
			}
			if isSQL(s) {
				lits[p] = append(lits[p], s)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return lits
}

var sqlShape = regexp.MustCompile(`\b(SELECT\b[\s\S]*\bFROM|INSERT\s+INTO|UPDATE\s+\S+\s+SET|DELETE\s+FROM|TRUNCATE|CREATE\s+TABLE)`)

func isSQL(s string) bool { return sqlShape.MatchString(s) }

var qualifiedName = regexp.MustCompile(`(?i)\b(identity|course|analytics|platform)\.[a-z_]+\b`)

// forbiddenUse builds the matcher for "SQL naming a table this service must not see".
func forbiddenUse(t *testing.T) (*regexp.Regexp, map[string]string) {
	tables := tablesBySchema(t)
	allowed := map[string]bool{}
	for _, s := range strings.Split(allowedSchemas, ",") {
		allowed[s] = true
	}
	var forbidden []string
	owner := map[string]string{}
	for schema, ts := range tables {
		if allowed[schema] {
			continue
		}
		for _, tb := range ts {
			forbidden = append(forbidden, regexp.QuoteMeta(tb))
			owner[tb] = schema
		}
	}
	sort.Strings(forbidden)
	return regexp.MustCompile(`(?i)\b(?:FROM|JOIN|INTO|UPDATE|TRUNCATE|TABLE|REFERENCES)\s+(?:ONLY\s+)?(?:"?[a-z_]+"?\.)?"?(` + strings.Join(forbidden, "|") + `)\b`), owner
}

func TestNoCrossSchemaSQLInProductionCode(t *testing.T) {
	use, owner := forbiddenUse(t)
	qualified := qualifiedName

	root := filepath.Join(repoRoot(t), "services", map[string]string{"course": "course", "identity": "identity", "analytics": "analytics"}[ownSchema])
	lits := sqlLiterals(t, root)
	total := 0
	for file, ss := range lits {
		for _, s := range ss {
			total++
			if m := use.FindStringSubmatch(s); m != nil {
				t.Errorf("%s: SQL references table %q owned by schema %q, which is not on this service's search_path (%s):\n%s", file, m[1], owner[m[1]], allowedSchemas, s)
			}
			if m := qualified.FindString(s); m != "" {
				t.Errorf("%s: SQL uses a schema-qualified name %q; services must rely on search_path (ADR-0024):\n%s", file, m, s)
			}
			if regexp.MustCompile(`(?i)\bsearch_path\b`).MatchString(s) {
				t.Errorf("%s: production SQL manipulates search_path:\n%s", file, s)
			}
		}
	}
	if total < 5 {
		t.Fatalf("only %d SQL literals found under %s; the scanner is not seeing the service's SQL", total, root)
	}
	t.Logf("scanned %d SQL literals in %d files", total, len(lits))
}

// The scanner must actually be able to catch a violation (mutation check):
// a query shaped like the pre-ADR-0024 cross-schema join must be flagged for
// every service except the schema that owns the table.
func TestScannerDetectsViolations(t *testing.T) {
	use, _ := forbiddenUse(t)
	foreign := map[string]string{
		"course":    "SELECT u.full_name FROM users u JOIN courses c ON c.instructor_id = u.id",
		"identity":  "SELECT c.id FROM courses c WHERE c.slug = ?",
		"analytics": "SELECT count(*) FROM enrollments WHERE course_id = ?",
	}
	q := foreign[ownSchema]
	if !isSQL(q) || !use.MatchString(q) {
		t.Fatalf("scanner failed to flag a cross-schema query for %s service: %s", ownSchema, q)
	}
	if !qualifiedName.MatchString("SELECT 1 FROM identity.users") {
		t.Fatal("qualified-name regexp missed identity.users")
	}
	if own := "SELECT 1 FROM " + map[string]string{"course": "courses", "identity": "users", "analytics": "course_stats"}[ownSchema]; use.MatchString(own) {
		t.Fatalf("scanner flagged the service's own table: %s", own)
	}
}
