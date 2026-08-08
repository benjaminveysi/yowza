//go:build !linux

package container

import (
	"fmt"
	"os"
	"os/exec"
)

// Sandbox defines an isolated environment using Linux namespaces.
// On non-Linux systems, it degrades to a standard process execution.
type Sandbox struct {
	Command []string
}

func NewSandbox(cmd ...string) *Sandbox {
	return &Sandbox{
		Command: cmd,
	}
}

// Run executes the command.
// On non-Linux systems, namespace isolation is not supported, so it just runs normally.
func (s *Sandbox) Run() error {
	if len(s.Command) == 0 {
		return fmt.Errorf("no command provided")
	}

	fmt.Println("[Sandbox Warning] Running on non-Linux OS. Container namespace isolation is disabled. Running as standard process.")

	cmd := exec.Command(s.Command[0], s.Command[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start process: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("process exited with error: %v", err)
	}

	return nil
}
