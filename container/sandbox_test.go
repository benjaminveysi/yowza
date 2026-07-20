package container

import (
	"os"
	"strings"
	"testing"
)

func TestSandboxExecution(t *testing.T) {
	// A simple test to verify the sandbox can execute a command.
	// Note: We cannot test true namespace isolation without root privileges,
	// so this test just ensures the wrapper doesn't crash on setup.

	// 'echo hello' is safe and works without root namespaces if we skip Cloneflags.
	// But since our wrapper hardcodes Cloneflags, it WILL fail without root.
	// Let's gracefully handle the permission error to ensure the code path is tested.

	sandbox := NewSandbox("echo", "hello")
	err := sandbox.Run()

	if err != nil {
		// If not root, we expect a permission error.
		if os.Getuid() != 0 {
			if !strings.Contains(err.Error(), "operation not permitted") {
				t.Fatalf("Expected 'operation not permitted' error for non-root user, got: %v", err)
			}
			t.Logf("Gracefully caught expected permission error for non-root namespace creation: %v", err)
		} else {
			// If we are root, it should actually succeed.
			t.Fatalf("Sandbox failed as root: %v", err)
		}
	} else {
		if os.Getuid() != 0 {
			t.Fatalf("Sandbox unexpectedly succeeded without root privileges. Cloneflags might be broken.")
		}
	}
}
