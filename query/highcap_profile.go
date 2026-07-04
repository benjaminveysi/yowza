// High-capacity query profile (worker Q, Yowza64 track).
//
// Two named, opt-in profiles govern the WASM batch executor:
//
//   - Standard (the default when nothing is configured):
//     exactly today's behaviour — 16 MiB per-operator state
//     buffers, the compile-time row/group caps (262144 /
//     65536), and the old failure semantics (a state-full
//     operator fails the query loudly; the state is
//     discarded). Standard runs on both wasm binaries and
//     its successful-query results are bit-identical to
//     before this change.
//
//   - HighCapacity (wasm64 ONLY): the host grows an
//     operator's state region geometrically on a state-full
//     return and retries the batch, under a host budget.
//     It refuses loudly on wasm32 — never silently
//     degrading to Standard — because wasm32 memory cannot
//     grow past 4 GiB and its growth path allocates (and
//     touches) heap pages, while wasm64 growth reserves
//     virtual address space and commits on touch.
//
// Budget formula (HighCapacity, when BudgetBytes is 0):
//
//	budget = min(MemTotal, cgroupLimit if one exists) / 4
//
// MemTotal comes from /proc/meminfo; the cgroup limit from
// /sys/fs/cgroup/memory.max (v2), falling back to the v1
// memory.limit_in_bytes file. The divisor 4 is deliberate:
// the executor shares the host with the Go runtime, the
// store, and the OS, and wasm memory is demand-paged —
// growth reserves address space but touching a page
// commits RAM. A quarter of host RAM is the largest
// ceiling this executor is willing to impose on itself
// without an operator explicitly asking for more via
// BudgetBytes. The budget is a ceiling on the executor's
// total wasm memory footprint (memGrowTo refuses targets
// above it), not a reservation: nothing is allocated up
// front. A phone stays on Standard unless explicitly
// opted into HighCapacity with a fitting budget; on a
// constrained host the formula above yields a small
// budget, and construction fails loudly if that budget
// cannot even hold the initial layout, rather than
// auto-selecting multi-GB budgets.
//
// Growth policy (host-owned; C stays dumb with a per-call
// cap): on a preflight OOM the host grows the state
// region to max(2 * old_cap, last_need), rounded up to a
// wasm page, relocates the region to the tail (see
// tailStart/tailEnd in isa_wasm_exec.go), and retries the
// same batch. Doubling bounds the number of relocations
// (logarithmic in final size) and last_need keeps growth
// from overshooting by more than 2x when the C preflight
// knows the requirement exactly. Growth stops loudly at
// the budget with ErrWasmBudgetExceeded: never a silent
// truncate, never an unbounded host allocation.
package query

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// BatchCapacity selects the executor's capacity profile.
// The zero value is Standard, so existing callers that
// never set Profile keep exactly today's behaviour.
type BatchCapacity int

const (
	BatchCapacityStandard BatchCapacity = iota
	BatchCapacityHigh
)

func (c BatchCapacity) String() string {
	switch c {
	case BatchCapacityStandard:
		return "standard"
	case BatchCapacityHigh:
		return "high-capacity"
	default:
		return fmt.Sprintf("unknown(%d)", int(c))
	}
}

// Standard profile constants (today's behaviour, kept exact).
const (
	StandardMaxRows   = 262144
	StandardMaxGroups = 65536
)

// HighCapacity profile defaults. StateCap is the INITIAL
// per-operator state region; growth (not a larger initial
// cap) is what carries heavy queries. WorkCap covers the
// sort-finish index arrays (2 * rows * 4 bytes) and the
// probe match array (rows * 4 bytes) at MaxRows:
// 8 MiB + 4 MiB, rounded up to 16 MiB. OutMax covers a
// full barrier emission at MaxRows for ordinary widths.
// MaxRows/MaxGroups are 4x the compile-time caps: large
// enough that the memory budget, not the caps, is the
// binding constraint, while keeping work buffers bounded.
const (
	HighCapacityStateCap  = 64 << 20
	HighCapacityWorkCap   = 16 << 20
	HighCapacityOutMax    = 256 << 20
	HighCapacityMaxRows   = 1048576
	HighCapacityMaxGroups = 262144
)

// HostBudgetFraction is the divisor in the host-aware
// budget formula (see the package comment).
const HostBudgetFraction = 4

// ErrWasmBudgetExceeded is returned (wrapped with the
// attempted size and the budget) when host-owned growth
// would take the executor's wasm memory past its budget.
// Callers test for it with errors.Is; it is never mapped
// to success, a partial result, or a silent downgrade.
var ErrWasmBudgetExceeded = errors.New("qbatch: wasm memory budget exceeded")

// hostMemoryCap returns min(MemTotal, cgroupLimit) in
// bytes, where the cgroup limit is the first finite limit
// found among cgroupPaths (a missing file means "no
// limit"; a file containing "max" means unlimited; a v1
// value at or above 2^62 is the kernel's "no limit"
// sentinel and is treated as unlimited). It errors when
// the meminfo file cannot be read or has no MemTotal,
// because a HighCapacity budget must never be guessed.
func hostMemoryCap(meminfoPath string, cgroupPaths ...string) (uint64, error) {
	data, err := os.ReadFile(meminfoPath)
	if err != nil {
		return 0, fmt.Errorf("qbatch high-capacity: read %s: %w", meminfoPath, err)
	}
	var total uint64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			v, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("qbatch high-capacity: parse MemTotal %q: %w", fields[1], err)
			}
			total = v * 1024
			break
		}
	}
	if total == 0 {
		return 0, fmt.Errorf("qbatch high-capacity: no MemTotal in %s", meminfoPath)
	}
	for _, path := range cgroupPaths {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue // no such file: no limit from this source
		}
		s := strings.TrimSpace(string(raw))
		if s == "" || s == "max" {
			continue
		}
		limit, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("qbatch high-capacity: parse cgroup limit %s %q: %w", path, s, err)
		}
		if limit >= 1<<62 {
			continue // kernel "no limit" sentinel
		}
		if limit < total {
			total = limit
		}
		// First finite limit wins: v2 and v1 describe the
		// same container, and taking the min twice is a
		// no-op once the smaller is applied.
		break
	}
	return total, nil
}

// HostMemoryBudget computes the default HighCapacity
// budget from the live host: hostMemoryCap / 4. An
// explicit BudgetBytes always wins over this value.
func HostMemoryBudget() (uint64, error) {
	total, err := hostMemoryCap("/proc/meminfo", "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0, err
	}
	return total / HostBudgetFraction, nil
}
