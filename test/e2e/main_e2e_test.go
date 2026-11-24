//go:build e2e

package e2e_test

import (
	"os"
	"testing"

	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// TestMain stops the containers the whole run shares (Schema Registry,
// Temporal) when the last scenario ends.
func TestMain(m *testing.M) { os.Exit(harness.Main(m)) }
