package binary

import (
	"bytes"
	"debug/dwarf"
	"errors"
	"fmt"
	"io"

	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/experimental"
	"github.com/tetratelabs/wazero/internal/leb128"
	"github.com/tetratelabs/wazero/internal/wasm"
	"github.com/tetratelabs/wazero/internal/wasmdebug"
)

// DecodeModule implements wasm.DecodeModule for the WebAssembly 1.0 (20191205) Binary Format
// See https://www.w3.org/TR/2019/REC-wasm-core-1-20191205/#binary-format%E2%91%A0
func DecodeModule(
	binary []byte,
	enabledFeatures api.CoreFeatures,
	memoryLimitPages uint32,
	memoryCapacityFromMax,
	dwarfEnabled, storeCustomSections bool,
) (*wasm.Module, error) {
	r := bytes.NewReader(binary)

	// Magic number.
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil || !bytes.Equal(buf, Magic) {
		return nil, ErrInvalidMagicNumber
	}

	// Version.
	if _, err := io.ReadFull(r, buf); err != nil || !bytes.Equal(buf, version) {
		return nil, ErrInvalidVersion
	}

	memSizer := newMemorySizer(memoryLimitPages, memoryCapacityFromMax)

	m := &wasm.Module{}
	var lastSectionID wasm.SectionID
	var info, line, str, abbrev, ranges []byte // For DWARF Data.
	for {
		// TODO: except custom sections, all others are required to be in order, but we aren't checking yet.
		// See https://www.w3.org/TR/2019/REC-wasm-core-1-20191205/#modules%E2%91%A0%E2%93%AA
		sectionID, err := r.ReadByte()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("read section id: %w", err)
		}

		sectionSize, _, err := leb128.DecodeUint32(r)
		if err != nil {
			return nil, fmt.Errorf("get size of section %s: %v", wasm.SectionIDName(sectionID), err)
		}

		var ok bool
		lastSectionID, ok = checkSectionOrder(sectionID, lastSectionID)
		if !ok {
			return nil, errors.New("invalid section order")
		}

		sectionContentStart := r.Len()
		switch sectionID {
		case wasm.SectionIDCustom:
			// First, validate the section and determine if the section for this name has already been set
			name, nameSize, decodeErr := decodeUTF8(r, "custom section name")
			if decodeErr != nil {
				err = decodeErr
				break
			} else if sectionSize < nameSize {
				err = fmt.Errorf("malformed custom section %s", name)
				break
			} else if name == "name" && m.NameSection != nil {
				err = fmt.Errorf("redundant custom section %s", name)
				break
			}

			// Now, either decode the NameSection or CustomSection
			limit := sectionSize - nameSize

			var c *wasm.CustomSection
			if name != "name" {
				if storeCustomSections || dwarfEnabled {
					c, err = decodeCustomSection(r, name, uint64(limit))
					if err != nil {
						return nil, fmt.Errorf("failed to read custom section name[%s]: %w", name, err)
					}
					m.CustomSections = append(m.CustomSections, c)
					if dwarfEnabled {
						switch name {
						case ".debug_info":
							info = c.Data
						case ".debug_line":
							line = c.Data
						case ".debug_str":
							str = c.Data
						case ".debug_abbrev":
							abbrev = c.Data
						case ".debug_ranges":
							ranges = c.Data
						}
					}
				} else {
					if _, err = io.CopyN(io.Discard, r, int64(limit)); err != nil {
						return nil, fmt.Errorf("failed to skip name[%s]: %w", name, err)
					}
				}
			} else {
				m.NameSection, err = decodeNameSection(r, uint64(limit))
			}
		case wasm.SectionIDType:
			m.TypeSection, err = decodeTypeSection(enabledFeatures, r)
		case wasm.SectionIDImport:
			m.ImportSection, m.ImportPerModule, m.ImportFunctionCount, m.ImportGlobalCount, m.ImportMemoryCount, m.ImportTableCount, m.ImportTagCount, err = decodeImportSection(r, memSizer, memoryLimitPages, enabledFeatures)
			if err != nil {
				return nil, err // avoid re-wrapping the error.
			}
		case wasm.SectionIDFunction:
			m.FunctionSection, err = decodeFunctionSection(r)
		case wasm.SectionIDTable:
			m.TableSection, err = decodeTableSection(r, enabledFeatures)
		case wasm.SectionIDMemory:
			m.MemorySection, err = decodeMemorySection(r, enabledFeatures, memSizer, memoryLimitPages)
		case wasm.SectionIDTag:
			if err := enabledFeatures.RequireEnabled(experimental.CoreFeaturesExceptionHandling); err != nil {
				return nil, fmt.Errorf("tag section not supported as %v", err)
			}
			m.TagSection, err = decodeTagSection(r)
		case wasm.SectionIDGlobal:
			if m.GlobalSection, err = decodeGlobalSection(r, enabledFeatures); err != nil {
				return nil, err // avoid re-wrapping the error.
			}
		case wasm.SectionIDExport:
			m.ExportSection, m.Exports, err = decodeExportSection(r)
		case wasm.SectionIDStart:
			m.StartSection, err = decodeStartSection(r)
		case wasm.SectionIDElement:
			m.ElementSection, err = decodeElementSection(r, enabledFeatures)
		case wasm.SectionIDCode:
			m.CodeSection, err = decodeCodeSection(r)
		case wasm.SectionIDData:
			m.DataSection, err = decodeDataSection(r, enabledFeatures)
		case wasm.SectionIDDataCount:
			if err := enabledFeatures.RequireEnabled(api.CoreFeatureBulkMemoryOperations); err != nil {
				return nil, fmt.Errorf("data count section not supported as %v", err)
			}
			m.DataCountSection, err = decodeDataCountSection(r)
		default:
			err = ErrInvalidSectionID
		}

		readBytes := sectionContentStart - r.Len()
		if err == nil && int(sectionSize) != readBytes {
			err = fmt.Errorf("invalid section length: expected to be %d but got %d", sectionSize, readBytes)
		}

		if err != nil {
			return nil, fmt.Errorf("section %s: %v", wasm.SectionIDName(sectionID), err)
		}
	}

	if dwarfEnabled {
		d, _ := dwarf.New(abbrev, nil, nil, info, line, nil, ranges, str)
		m.DWARFLines = wasmdebug.NewDWARFLines(d)
	}

	functionCount, codeCount := m.SectionElementCount(wasm.SectionIDFunction), m.SectionElementCount(wasm.SectionIDCode)
	if functionCount != codeCount {
		return nil, fmt.Errorf("function and code section have inconsistent lengths: %d != %d", functionCount, codeCount)
	}
	return m, nil
}

func checkSectionOrder(current, previous wasm.SectionID) (byte, bool) {
	// https://webassembly.github.io/spec/core/binary/modules.html#binary-module

	// Custom sections can show up anywhere.
	if current == wasm.SectionIDCustom {
		return previous, true
	}

	// Tag section (ID 13) must come after Memory (5) and before Global (6).
	if current == wasm.SectionIDTag {
		return current, previous <= wasm.SectionIDMemory
	}
	if previous == wasm.SectionIDTag {
		return current, current >= wasm.SectionIDGlobal
	}

	// DataCount was introduced in Wasm 2.0.
	// It must come after Element and before Code.
	if current > wasm.SectionIDDataCount {
		return current, false
	}
	if current == wasm.SectionIDDataCount {
		return current, previous <= wasm.SectionIDElement
	}
	if previous == wasm.SectionIDDataCount {
		return current, current >= wasm.SectionIDCode
	}

	// Otherwise, strictly increasing order.
	return current, current > previous
}

// memorySizer derives min, capacity and max pages from decoded wasm.
type memorySizer func(minPages uint64, maxPages *uint64) (min uint64, capacity uint64, max uint64)

// newMemorySizer sets capacity to minPages unless max is defined and
// memoryCapacityFromMax is true.
//
// For a memory without a max, the capacity is capped at memoryLimitPages
// (65536 pages = 4 GiB by default), even for memory64. That cap is a host
// policy, not a limit of the memory64 standard: the standard allows far
// larger memories, but reserving more than 4 GiB by default would commit the
// host to a huge virtual reservation for every max-less memory64 module.
// Callers that need larger memories must set an explicit max or raise the
// limit via WithMemoryLimitPages; do not raise this default to 1<<48 here.
func newMemorySizer(memoryLimitPages uint32, memoryCapacityFromMax bool) memorySizer {
	return func(minPages uint64, maxPages *uint64) (min, capacity, max uint64) {
		if maxPages != nil {
			if memoryCapacityFromMax {
				return minPages, *maxPages, *maxPages
			}
			// This is an invalid value: let it propagate, we will fail later.
			if *maxPages > uint64(wasm.MemoryLimitPages) {
				return minPages, minPages, *maxPages
			}
			// This is a valid value, but it goes over the run-time limit: return the limit.
			if *maxPages > uint64(memoryLimitPages) {
				return minPages, minPages, uint64(memoryLimitPages)
			}
			return minPages, minPages, *maxPages
		}
		if memoryCapacityFromMax {
			return minPages, uint64(memoryLimitPages), uint64(memoryLimitPages)
		}
		return minPages, minPages, uint64(memoryLimitPages)
	}
}
