// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package arrowipc

import (
	"encoding/binary"
	"errors"
)

var (
	ErrInvalid = errors.New("invalid Arrow IPC metadata")
	ErrLimit   = errors.New("Arrow IPC metadata limit exceeded")
)

const maxIPCMetadata = 8 << 20

const (
	maxIPCFields  = 4096
	maxIPCDepth   = 64
	maxIPCBuffers = 32768
)

// Arrow-Go's FlatBuffer accessors are not verifiers. Schema conversion allocates
// Go slices/strings outside its Allocator, using vector lengths in metadata.
// Validate allocation-critical fields against both their encoded extents and
// explicit complexity limits before invoking Arrow's semantic decoder.
// Field indexes follow Apache Arrow format/{Message,Schema}.fbs (IPC V4/V5).
func ValidateMessageMetadata(data []byte) (bodyLength int64, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recovered == ErrLimit {
				err = ErrLimit
			} else {
				err = ErrInvalid
			}
		}
	}()
	v := metadataCheck{data: data}
	message := v.table(v.pointer(0))
	version := v.scalar(message, 0, 2)
	if version != 3 && version != 4 { // Arrow MetadataVersion V4 and V5.
		panic(ErrInvalid)
	}
	bodyLength = int64(v.scalar(message, 3, 8))
	if bodyLength < 0 {
		panic(ErrInvalid)
	}
	v.keyValues(message, 4)
	header := v.table(v.reference(message, 2, true))
	switch v.scalar(message, 1, 1) {
	case 1: // Schema
		if bodyLength != 0 {
			panic(ErrInvalid)
		}
		v.schema(header)
	case 2: // DictionaryBatch
		v.scalar(header, 0, 8)
		v.scalar(header, 2, 1)
		v.record(v.table(v.reference(header, 1, true)), bodyLength)
	case 3: // RecordBatch
		v.record(header, bodyLength)
	default:
		panic(ErrInvalid)
	}
	return bodyLength, nil
}

type metadataCheck struct {
	data    []byte
	fields  int
	strings int
	pairs   int
}

type metadataTable struct {
	position, vtable, vsize, objectSize int
}

func (v *metadataCheck) bytes(position, size int) []byte {
	if position < 0 || size < 0 || size > len(v.data) || position > len(v.data)-size {
		panic(ErrInvalid)
	}
	return v.data[position : position+size]
}

func (v *metadataCheck) pointer(position int) int {
	distance := uint64(binary.LittleEndian.Uint32(v.bytes(position, 4)))
	target := uint64(position) + distance
	if distance == 0 || target >= uint64(len(v.data)) {
		panic(ErrInvalid)
	}
	return int(target)
}

func (v *metadataCheck) table(position int) metadataTable {
	distance := int64(int32(binary.LittleEndian.Uint32(v.bytes(position, 4))))
	vt := int64(position) - distance
	if vt < 0 || vt > int64(len(v.data))-4 {
		panic(ErrInvalid)
	}
	header := v.bytes(int(vt), 4)
	t := metadataTable{position: position, vtable: int(vt), vsize: int(binary.LittleEndian.Uint16(header)), objectSize: int(binary.LittleEndian.Uint16(header[2:]))}
	if t.vsize < 4 || t.vsize%2 != 0 || t.objectSize < 4 {
		panic(ErrInvalid)
	}
	v.bytes(t.vtable, t.vsize)
	v.bytes(t.position, t.objectSize)
	return t
}

func (v *metadataCheck) field(t metadataTable, index, width int) int {
	entry := 4 + index*2
	if entry >= t.vsize {
		return 0
	}
	offset := int(binary.LittleEndian.Uint16(v.bytes(t.vtable+entry, 2)))
	if offset == 0 {
		return 0
	}
	if offset < 4 || width > t.objectSize || offset > t.objectSize-width {
		panic(ErrInvalid)
	}
	v.bytes(t.position+offset, width)
	return t.position + offset
}

func (v *metadataCheck) scalar(t metadataTable, index, width int) uint64 {
	position := v.field(t, index, width)
	if position == 0 {
		return 0
	}
	b := v.bytes(position, width)
	switch width {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return binary.LittleEndian.Uint64(b)
	}
	panic(ErrInvalid)
}

func (v *metadataCheck) reference(t metadataTable, index int, required bool) int {
	position := v.field(t, index, 4)
	if position == 0 {
		if required {
			panic(ErrInvalid)
		}
		return 0
	}
	return v.pointer(position)
}

func (v *metadataCheck) vector(t metadataTable, index, stride, maximum int) (position, count int) {
	position = v.reference(t, index, false)
	if position == 0 {
		return 0, 0
	}
	n := uint64(binary.LittleEndian.Uint32(v.bytes(position, 4)))
	if n > uint64(maximum) {
		panic(ErrLimit)
	}
	position += 4
	if n*uint64(stride) > uint64(len(v.data)-position) {
		panic(ErrInvalid)
	}
	return position, int(n)
}

func (v *metadataCheck) text(t metadataTable, index int) {
	_, count := v.vector(t, index, 1, maxIPCMetadata)
	v.strings += count
	if v.strings > maxIPCMetadata {
		panic(ErrLimit)
	}
}

func (v *metadataCheck) keyValues(t metadataTable, index int) {
	position, count := v.vector(t, index, 4, maxIPCFields)
	v.pairs += count
	if v.pairs > maxIPCFields {
		panic(ErrLimit)
	}
	for i := 0; i < count; i++ {
		pair := v.table(v.pointer(position + i*4))
		v.text(pair, 0)
		v.text(pair, 1)
	}
}

func (v *metadataCheck) schema(t metadataTable) {
	v.scalar(t, 0, 2)
	v.keyValues(t, 2)
	v.vector(t, 3, 8, 64)
	position, count := v.vector(t, 1, 4, maxIPCFields)
	for i := 0; i < count; i++ {
		v.arrowField(v.table(v.pointer(position+i*4)), 1)
	}
}

func (v *metadataCheck) arrowField(t metadataTable, depth int) {
	v.fields++
	if depth > maxIPCDepth || v.fields > maxIPCFields {
		panic(ErrLimit)
	}
	v.text(t, 0)
	v.scalar(t, 1, 1)
	v.keyValues(t, 6)
	typeTable := v.table(v.reference(t, 3, true))
	typeID := v.scalar(t, 2, 1)
	switch typeID {
	case 1, 4, 5, 6, 12, 13, 19, 20, 21, 22, 23, 24, 25, 26:
	case 2: // Int
		v.scalar(typeTable, 0, 4)
		v.scalar(typeTable, 1, 1)
	case 3, 8, 11, 18: // FloatingPoint, Date, Interval, Duration
		v.scalar(typeTable, 0, 2)
	case 7: // Decimal
		v.scalar(typeTable, 0, 4)
		v.scalar(typeTable, 1, 4)
		v.scalar(typeTable, 2, 4)
	case 9: // Time
		v.scalar(typeTable, 0, 2)
		v.scalar(typeTable, 1, 4)
	case 10: // Timestamp
		v.scalar(typeTable, 0, 2)
		v.text(typeTable, 1)
	case 14: // Union
		v.scalar(typeTable, 0, 2)
		v.vector(typeTable, 1, 4, maxIPCFields)
	case 15, 16: // FixedSizeBinary, FixedSizeList
		v.scalar(typeTable, 0, 4)
	case 17: // Map
		v.scalar(typeTable, 0, 1)
	default:
		panic(ErrInvalid)
	}
	if pos := v.reference(t, 4, false); pos != 0 {
		dict := v.table(pos)
		v.scalar(dict, 0, 8)
		v.scalar(dict, 2, 1)
		v.scalar(dict, 3, 2)
		indexType := v.table(v.reference(dict, 1, true))
		v.scalar(indexType, 0, 4)
		v.scalar(indexType, 1, 1)
	}
	position, count := v.vector(t, 5, 4, maxIPCFields)
	for i := 0; i < count; i++ {
		v.arrowField(v.table(v.pointer(position+i*4)), depth+1)
	}
}

func (v *metadataCheck) record(t metadataTable, bodyLength int64) {
	if int64(v.scalar(t, 0, 8)) < 0 {
		panic(ErrInvalid)
	}
	position, count := v.vector(t, 1, 16, maxIPCFields)
	for i := 0; i < count; i++ {
		node := v.bytes(position+i*16, 16)
		length := int64(binary.LittleEndian.Uint64(node))
		nulls := int64(binary.LittleEndian.Uint64(node[8:]))
		if length < 0 || nulls < 0 || nulls > length {
			panic(ErrInvalid)
		}
	}
	position, count = v.vector(t, 2, 16, maxIPCBuffers)
	for i := 0; i < count; i++ {
		buffer := v.bytes(position+i*16, 16)
		offset := int64(binary.LittleEndian.Uint64(buffer))
		length := int64(binary.LittleEndian.Uint64(buffer[8:]))
		if offset < 0 || length < 0 || offset > bodyLength || length > bodyLength-offset {
			panic(ErrInvalid)
		}
	}
	if pos := v.reference(t, 3, false); pos != 0 {
		compression := v.table(pos)
		v.scalar(compression, 0, 1)
		v.scalar(compression, 1, 1)
	}
	position, count = v.vector(t, 4, 8, maxIPCFields)
	var buffers uint64
	for i := 0; i < count; i++ {
		n := binary.LittleEndian.Uint64(v.bytes(position+i*8, 8))
		if n > maxIPCBuffers || buffers+n > maxIPCBuffers {
			panic(ErrLimit)
		}
		buffers += n
	}
}
