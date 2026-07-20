//go:build linux

package container

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
)

const sandboxInitSentinel = "__yowza_sandbox_init__"

// init intercepts the self-re-exec child inside the newly created
// PID/mount namespaces BEFORE main() runs in any importing binary (such
// as yowzactl or a go test binary): it privately remounts /proc for the
// new PID namespace and execs the target binary in-place as PID 1.
//
// It returns immediately unless os.Args[1] is exactly the sentinel, so
// normal runs of any binary importing this package (go test, yowzactl,
// ...) are completely unaffected. When the sentinel is present but no
// target command follows, it exits 127 rather than falling through to
// main() with a meaningless argument.
func init() {
	if len(os.Args) < 3 || os.Args[1] != sandboxInitSentinel {
		return
	}
	// Args layout from Run(): [sentinel, rootfs, target...]. The rootfs
	// is passed as an argument (not applied via SysProcAttr.Chroot)
	// because the kernel applies SysProcAttr.Chroot BEFORE exec'ing
	// /proc/self/exe, and inside the new root /proc/self/exe would no
	// longer resolve. Doing the chroot here, after the re-exec, lets
	// /proc be mounted inside the new root afterwards.
	rootfs := os.Args[2]
	targetCmd := os.Args[3:]
	if len(targetCmd) == 0 {
		fmt.Fprintf(os.Stderr, "[yowza-sandbox-init] no target command\n")
		os.Exit(127)
	}

	// 1. Make mount propagation private so mounting /proc inside the
	// namespace never propagates back to the host's /proc.
	_ = syscall.Mount("", "/", "", syscall.MS_PRIVATE|syscall.MS_REC, "")

	// 1b. Chroot into the requested rootfs (if any) BEFORE mounting
	// /proc, so /proc is mounted inside the new root, not the host's.
	// LookPath/Exec below therefore resolve inside the chroot, which
	// is the point of a chroot sandbox. Exit 125 (runc convention)
	// on chroot/chdir failure.
	if rootfs != "" && rootfs != "/" {
		if err := syscall.Chroot(rootfs); err != nil {
			fmt.Fprintf(os.Stderr, "[yowza-sandbox-init] chroot failed: %v\n", err)
			os.Exit(125)
		}
		if err := os.Chdir("/"); err != nil {
			fmt.Fprintf(os.Stderr, "[yowza-sandbox-init] chdir / failed: %v\n", err)
			os.Exit(125)
		}
	}

	// 2. Mount /proc inside the new PID namespace so tools reading
	// /proc see only sandboxed processes. Failure here is non-fatal
	// (the target still runs, but with the host /proc visible); the
	// caller can detect that by inspecting /proc inside the sandbox.
	// Note: the rootfs must contain a /proc directory for this to
	// succeed in the chroot case; a rootfs without one simply keeps
	// the old behaviour (no remount, target still runs).
	if st, err := os.Stat("/proc"); err == nil && st.IsDir() {
		_ = syscall.Mount("proc", "/proc", "proc",
			syscall.MS_NOSUID|syscall.MS_NOEXEC|syscall.MS_NODEV, "")
	}

	// 3. Resolve and exec the target binary in-place (preserving PID 1).
	binPath, err := exec.LookPath(targetCmd[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "[yowza-sandbox-init] command not found: %v\n", err)
		os.Exit(127)
	}
	if err := syscall.Exec(binPath, targetCmd, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "[yowza-sandbox-init] exec failed: %v\n", err)
		os.Exit(126)
	}
}

// Sandbox defines an isolated environment using Linux namespaces.
type Sandbox struct {
	Command []string

	// IsolateNetwork, when true, adds CLONE_NEWNET. It defaults to
	// false (zero value): a daemon that must listen on a host port
	// cannot run inside a fresh network namespace, whose loopback
	// starts DOWN and which has no host interfaces. Callers that want
	// a network-isolated process must set this explicitly.
	IsolateNetwork bool

	// RootFS, when set to anything other than "" or "/", is passed as
	// SysProcAttr.Chroot for the child process. It is not used by
	// default: chrooting into "/" is a no-op and chrooting into an
	// arbitrary directory without a prepared rootfs would break the
	// command (missing libraries, /dev, ...).
	RootFS string

	// MemoryLimitMB, when > 0, applies a best-effort cgroup v2 memory
	// limit (memory.max) to the child after it starts. If /sys/fs/cgroup
	// is not writable, the limit is silently skipped, never fatal: a
	// sandbox must not fail to start only because cgroups are unavailable.
	MemoryLimitMB int
}

func NewSandbox(cmd ...string) *Sandbox {
	return &Sandbox{
		Command: cmd,
	}
}

// Run executes the command inside a new set of namespaces
// (PID, Mount, UTS, IPC, Network).
// Note: This requires root privileges (CAP_SYS_ADMIN) on Linux.
//
// This is NOT full isolation yet. What is implemented here, and what
// is not, stated precisely:
//   - Namespaces UTS/PID/NS/IPC are always applied; NET only when
//     IsolateNetwork is set (see the field comment: a fresh network
//     namespace starts with loopback DOWN).
//   - Chroot: applied by init() itself (not via SysProcAttr.Chroot,
//     which would run before the /proc/self/exe re-exec and break
//     it) when RootFS is set to a real directory (not "" or "/").
//     There is no pivot_root.
//   - Cgroups: a best-effort v2 memory.max limit when MemoryLimitMB
//     is set; silently skipped when /sys/fs/cgroup is not writable.
//   - /proc IS mounted inside the new mount+PID namespace via the
//     self-re-exec init() above (the child-init helper that
//     exec.Command alone cannot provide): whenever /proc/self/exe
//     exists, Run() re-execs the current binary with the sentinel
//     argument, init() makes propagation private, chroots if a
//     RootFS was given, mounts /proc (inside the new root when
//     chrooted — the rootfs must contain a /proc directory),
//     then execs the target as PID 1.
//
// In particular, the command run inside the sandbox must NOT be
// `go run`: `go run` needs the Go toolchain and the host filesystem
// (module cache, source tree) at run time, which defeats the point of
// the sandbox and couples it to the host; run a prebuilt binary
// instead (see cmd/yowzactl/main.go, which no longer uses go run).
func (s *Sandbox) Run() error {
	if len(s.Command) == 0 {
		return fmt.Errorf("no command provided")
	}

	// Always re-exec /proc/self/exe when it exists, passing RootFS as
	// an argument: init() (running after the re-exec, still on the
	// host root) chroots itself and then mounts /proc inside the new
	// root. Applying SysProcAttr.Chroot instead would break the
	// re-exec, because the kernel chroots before exec'ing
	// /proc/self/exe. Only when /proc/self/exe does not exist at all
	// is the direct path kept (then /proc is not remounted).
	var cmd *exec.Cmd
	if hasProcSelfExe() {
		reexecArgs := append([]string{sandboxInitSentinel, s.RootFS}, s.Command...)
		cmd = exec.Command("/proc/self/exe", reexecArgs...)
	} else {
		cmd = exec.Command(s.Command[0], s.Command[1:]...)
	}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Isolate namespaces:
	// CLONE_NEWUTS: Hostname/Domain name
	// CLONE_NEWPID: Process IDs
	// CLONE_NEWNS: Mount points
	// CLONE_NEWIPC: System V IPC objects and POSIX message queues
	//   (the old comment claimed IPC isolation, but CLONE_NEWIPC was
	//   never actually passed; it is now)
	// CLONE_NEWNET: only when IsolateNetwork is set. A new network
	// namespace starts with only loopback down, so a sandboxed daemon
	// has no network access unless it explicitly brings an interface up.
	cloneFlags := uintptr(syscall.CLONE_NEWUTS | syscall.CLONE_NEWPID | syscall.CLONE_NEWNS |
		syscall.CLONE_NEWIPC)
	if s.IsolateNetwork {
		cloneFlags |= syscall.CLONE_NEWNET
	}
	attr := &syscall.SysProcAttr{
		Cloneflags: cloneFlags,
	}
	// NOTE: no attr.Chroot here on purpose — see init() above.
	cmd.SysProcAttr = attr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start sandboxed process: %v", err)
	}

	// Best-effort cgroup v2 memory limit: failures are ignored on
	// purpose (see the MemoryLimitMB field comment).
	if s.MemoryLimitMB > 0 && cmd.Process != nil {
		applyCgroupV2MemoryLimit(cmd.Process.Pid, s.MemoryLimitMB)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("sandboxed process exited with error: %v", err)
	}

	return nil
}

func hasProcSelfExe() bool {
	_, err := os.Stat("/proc/self/exe")
	return err == nil
}

// applyCgroupV2MemoryLimit creates /sys/fs/cgroup/yowza-<pid> and writes
// memory.max and cgroup.procs for it. Every step is best-effort: on a
// system without writable cgroup v2, all of these fail and the caller
// simply runs without a limit.
func applyCgroupV2MemoryLimit(pid int, limitMB int) {
	cgDir := filepath.Join("/sys/fs/cgroup", fmt.Sprintf("yowza-%d", pid))
	if err := os.MkdirAll(cgDir, 0755); err != nil {
		return
	}
	limitBytes := int64(limitMB) * 1024 * 1024
	_ = os.WriteFile(filepath.Join(cgDir, "memory.max"), []byte(strconv.FormatInt(limitBytes, 10)), 0644)
	_ = os.WriteFile(filepath.Join(cgDir, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0644)
}
