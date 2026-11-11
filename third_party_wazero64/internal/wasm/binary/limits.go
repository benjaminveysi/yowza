package binary

import (
	"bytes"
	"fmt"

	"github.com/tetratelabs/wazero/internal/leb128"
)

// decodeLimitsType returns the `limitsType` (min, max) decoded with the WebAssembly 1.0 (20191205) Binary Format.
//
// See https://www.w3.org/TR/2019/REC-wasm-core-1-20191205/#limits%E2%91%A6
//
// Extended in threads proposal: https://webassembly.github.io/threads/core/binary/types.html#limits
func decodeLimitsType(r *bytes.Reader) (min uint32, max *uint32, shared bool, err error) {
	var flag byte
	if flag, err = r.ReadByte(); err != nil {
		err = fmt.Errorf("read leading byte: %v", err)
		return
	}

	switch flag {
	case 0x00, 0x02:
		min, _, err = leb128.DecodeUint32(r)
		if err != nil {
			err = fmt.Errorf("read min of limit: %v", err)
		}
	case 0x01, 0x03:
		min, _, err = leb128.DecodeUint32(r)
		if err != nil {
			err = fmt.Errorf("read min of limit: %v", err)
			return
		}
		var m uint32
		if m, _, err = leb128.DecodeUint32(r); err != nil {
			err = fmt.Errorf("read max of limit: %v", err)
		} else {
			max = &m
		}
	default:
		err = fmt.Errorf("%v for limits: %#x not in (0x00, 0x01, 0x02, 0x03)", ErrInvalidByte, flag)
	}

	shared = flag == 0x02 || flag == 0x03

	return
}

// decodeMemoryLimitsType returns the memory `limitsType` (min, max) decoded in WebAssembly Binary Format.
//
// This reads the memory64 proposal flags (0x04, 0x05, 0x06, 0x07) and reads bounds as 64-bit LEB128.
func decodeMemoryLimitsType(r *bytes.Reader) (min uint64, max *uint64, shared bool, is64 bool, err error) {
	var flag byte
	if flag, err = r.ReadByte(); err != nil {
		err = fmt.Errorf("read leading byte: %v", err)
		return
	}

	switch flag {
	case 0x00, 0x02: // 32-bit memory without max, or shared
		var m32 uint32
		m32, _, err = leb128.DecodeUint32(r)
		if err != nil {
			err = fmt.Errorf("read min of limit: %v", err)
		}
		min = uint64(m32)
	case 0x01, 0x03: // 32-bit memory with max, or shared
		var m32 uint32
		m32, _, err = leb128.DecodeUint32(r)
		if err != nil {
			err = fmt.Errorf("read min of limit: %v", err)
			return
		}
		min = uint64(m32)
		var max32 uint32
		if max32, _, err = leb128.DecodeUint32(r); err != nil {
			err = fmt.Errorf("read max of limit: %v", err)
		} else {
			m := uint64(max32)
			max = &m
		}
	case 0x04, 0x06: // 64-bit memory without max, or shared
		is64 = true
		min, _, err = leb128.DecodeUint64(r)
		if err != nil {
			err = fmt.Errorf("read min of limit: %v", err)
		}
	case 0x05, 0x07: // 64-bit memory with max, or shared
		is64 = true
		min, _, err = leb128.DecodeUint64(r)
		if err != nil {
			err = fmt.Errorf("read min of limit: %v", err)
			return
		}
		var max64 uint64
		if max64, _, err = leb128.DecodeUint64(r); err != nil {
			err = fmt.Errorf("read max of limit: %v", err)
		} else {
			max = &max64
		}
	default:
		err = fmt.Errorf("%v for limits: %#x not in (0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07)", ErrInvalidByte, flag)
	}

	shared = flag == 0x02 || flag == 0x03 || flag == 0x06 || flag == 0x07

	return
}
