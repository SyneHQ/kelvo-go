// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"encoding/binary"

	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	maxIPCMetadata = 1 << 20
	maxIPCFields   = 1024
	maxIPCDepth    = 16
	maxIPCBuffers  = 8192
)

// The Arrow reader allocates schema vectors before validating their lengths.
// This small preflight checks the wire format without allocating from its counts.
// Slot numbers follow the public Arrow Schema.fbs and Message.fbs definitions.
// Supported storage types are scalars, binary/string, lists, structs, maps and
// dictionaries. Union, view and run-end encoded layouts fail closed.
type fbCheck struct {
	b                   []byte
	fields, pairs, text int
	maxRows, maxBody    int64
	dictionaries        map[int64]ipcLayout
}

type ipcLayout struct {
	nodes, buffers, variadic int
	hasDictionary            bool
}

func (l *ipcLayout) add(child ipcLayout) {
	l.nodes += child.nodes
	l.buffers += child.buffers
	l.variadic += child.variadic
	l.hasDictionary = l.hasDictionary || child.hasDictionary
}

type fbTable struct {
	c      *fbCheck
	p, v   int
	fields int
}

type ipcBatch struct {
	rows         int64
	nodes        [][2]int64
	buffers      [][2]int64
	variadic     []int64
	compressed   bool
	codec        byte
	dictionary   bool
	id           int64
	delta        bool
	schema       *ipcLayout
	dictionaries map[int64]ipcLayout
}

func badIPC()     { panic(failure("PROTOCOL_ERROR")) }
func limitedIPC() { panic(failure("RESOURCE_EXHAUSTED")) }

func (c *fbCheck) span(p, n int) []byte {
	if p < 0 || n < 0 || p > len(c.b) || n > len(c.b)-p {
		badIPC()
	}
	return c.b[p : p+n]
}
func (c *fbCheck) u32(p int) uint32 { return binary.LittleEndian.Uint32(c.span(p, 4)) }
func (c *fbCheck) i64(p int) int64  { return int64(binary.LittleEndian.Uint64(c.span(p, 8))) }
func (c *fbCheck) indirect(p int) int {
	n := c.u32(p)
	if n < 4 || uint64(n) > uint64(len(c.b)-p) {
		badIPC()
	}
	return p + int(n)
}
func (c *fbCheck) table(p int, widths ...int) fbTable {
	delta := int64(int32(c.u32(p)))
	v := int64(p) - delta
	if v < 0 || v > int64(len(c.b))-4 {
		badIPC()
	}
	vt := c.span(int(v), 4)
	n, size := int(binary.LittleEndian.Uint16(vt)), int(binary.LittleEndian.Uint16(vt[2:]))
	if n < 4 || n%2 != 0 || n > 4+2*len(widths) || size < 4 {
		badIPC()
	}
	c.span(int(v), n)
	c.span(p, size)
	t := fbTable{c: c, p: p, v: int(v), fields: (n - 4) / 2}
	for i := 0; i < t.fields; i++ {
		o := int(binary.LittleEndian.Uint16(c.span(t.v+4+i*2, 2)))
		if o != 0 && (o < 4 || o > size || widths[i] > size-o) {
			badIPC()
		}
	}
	return t
}
func (t fbTable) slot(i int) int {
	if i >= t.fields {
		return 0
	}
	o := int(binary.LittleEndian.Uint16(t.c.span(t.v+4+i*2, 2)))
	if o == 0 {
		return 0
	}
	return t.p + o
}
func (t fbTable) uint(i, width int) uint64 {
	p := t.slot(i)
	if p == 0 {
		return 0
	}
	switch width {
	case 1:
		return uint64(t.c.span(p, 1)[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(t.c.span(p, 2)))
	case 4:
		return uint64(t.c.u32(p))
	case 8:
		return uint64(t.c.i64(p))
	}
	badIPC()
	return 0
}
func (t fbTable) ref(i int, required bool) int {
	p := t.slot(i)
	if p == 0 {
		if required {
			badIPC()
		}
		return 0
	}
	return t.c.indirect(p)
}
func (t fbTable) vector(i, width, limit int) (int, int) {
	p := t.ref(i, false)
	if p == 0 {
		return 0, 0
	}
	n := uint64(t.c.u32(p))
	if n > uint64(limit) {
		limitedIPC()
	}
	if n > uint64((len(t.c.b)-p-4)/width) {
		badIPC()
	}
	return p + 4, int(n)
}
func (t fbTable) str(i int) string {
	p, n := t.vector(i, 1, maxIPCMetadata)
	if p == 0 {
		return ""
	}
	if t.c.span(p+n, 1)[0] != 0 {
		badIPC()
	}
	if n > maxIPCMetadata-t.c.text {
		limitedIPC()
	}
	t.c.text += n
	return string(t.c.span(p, n))
}
func (c *fbCheck) metadata(t fbTable, slot int) {
	p, n := t.vector(slot, 4, maxIPCFields)
	if n > maxIPCFields-c.pairs {
		limitedIPC()
	}
	c.pairs += n
	for i := 0; i < n; i++ {
		kv := c.table(c.indirect(p+i*4), 4, 4)
		key := kv.str(0)
		kv.str(1)
		// Do not run globally registered extension deserializers on wire data.
		if key == "ARROW:extension:name" || key == "ARROW:extension:metadata" {
			badIPC()
		}
	}
}
func (c *fbCheck) field(p, depth int) ipcLayout {
	c.fields++
	if depth > maxIPCDepth || c.fields > maxIPCFields {
		limitedIPC()
	}
	f := c.table(p, 4, 1, 1, 4, 4, 4, 4)
	f.str(0)
	c.metadata(f, 6)
	kids, count := f.vector(5, 4, maxIPCFields)
	var children ipcLayout
	for i := 0; i < count; i++ {
		children.add(c.field(c.indirect(kids+i*4), depth+1))
	}
	typ := byte(f.uint(2, 1))
	p = f.ref(3, true)
	switch typ {
	case 1, 4, 5, 6, 12, 13, 19, 20, 21:
		c.table(p)
	case 2:
		c.table(p, 4, 1)
	case 3, 8, 11, 18:
		c.table(p, 2)
	case 7:
		t := c.table(p, 4, 4, 4)
		width := t.uint(2, 4)
		if t.slot(2) == 0 {
			width = 128
		}
		maximum := map[uint64]int32{32: 9, 64: 18, 128: 38, 256: 76}[width]
		precision, scale := int32(t.uint(0, 4)), int32(t.uint(1, 4))
		// Arrow constructs decimal types without validating these scalars.
		// A huge scale can amplify a tiny value into a huge formatted string.
		if maximum == 0 || precision < 1 || precision > maximum || scale < -76 || scale > 76 {
			badIPC()
		}
	case 9:
		c.table(p, 2, 4)
	case 10:
		t := c.table(p, 2, 4)
		t.str(1)
	case 15, 16:
		t := c.table(p, 4)
		n := int64(int32(t.uint(0, 4)))
		if n < 0 || n > c.maxBody {
			limitedIPC()
		}
	case 17:
		c.table(p, 1)
	default:
		badIPC()
	}
	layout := ipcLayout{nodes: 1}
	switch typ {
	case 1: // Null has no physical buffers.
	case 4, 5, 19, 20:
		layout.buffers = 3
	case 12, 17, 21:
		if count != 1 {
			badIPC()
		}
		layout.buffers = 2
		layout.add(children)
	case 16:
		if count != 1 {
			badIPC()
		}
		layout.buffers = 1
		layout.add(children)
	case 13:
		layout.buffers = 1
		layout.add(children)
	default:
		layout.buffers = 2
	}
	switch typ {
	case 12, 13, 16, 17, 21:
	default:
		if count != 0 {
			badIPC()
		}
	}
	if p = f.ref(4, false); p != 0 {
		if layout.hasDictionary {
			badIPC()
		}
		d := c.table(p, 8, 4, 1, 2)
		c.table(d.ref(1, true), 4, 1)
		id := int64(d.uint(0, 8))
		if previous, ok := c.dictionaries[id]; ok && previous != layout {
			badIPC()
		}
		c.dictionaries[id] = layout
		return ipcLayout{nodes: 1, buffers: 2, hasDictionary: true}
	}
	return layout
}

func checkIPCMetadata(raw []byte, cfg Config) (body int64, batch *ipcBatch) {
	c := &fbCheck{b: raw, maxRows: cfg.MaxRows, maxBody: cfg.MaxDecodedBytes, dictionaries: make(map[int64]ipcLayout)}
	m := c.table(c.indirect(0), 2, 1, 4, 8, 4)
	// Kelvo emits modern IPC V5; fail closed on unsupported revisions.
	if m.uint(0, 2) != 4 {
		badIPC()
	}
	c.metadata(m, 4)
	body = int64(m.uint(3, 8))
	if body < 0 || body%8 != 0 {
		badIPC()
	}
	if body > cfg.MaxDecodedBytes || body > cfg.MaxWireBytes {
		limitedIPC()
	}
	p := m.ref(2, true)
	switch m.uint(1, 1) {
	case 1:
		if body != 0 {
			badIPC()
		}
		s := c.table(p, 2, 4, 4, 4)
		if s.uint(0, 2) != 0 {
			badIPC()
		}
		c.metadata(s, 2)
		features, n := s.vector(3, 8, 16)
		for i := 0; i < n; i++ {
			if c.i64(features+i*8) < 0 || c.i64(features+i*8) > 2 {
				badIPC()
			}
		}
		fields, n := s.vector(1, 4, maxIPCFields)
		layout := &ipcLayout{}
		for i := 0; i < n; i++ {
			layout.add(c.field(c.indirect(fields+i*4), 1))
		}
		return body, &ipcBatch{schema: layout, dictionaries: c.dictionaries}
	case 2:
		d := c.table(p, 8, 4, 1)
		batch = &ipcBatch{dictionary: true, id: int64(d.uint(0, 8)), delta: d.uint(2, 1) != 0}
		p = d.ref(1, true)
	case 3:
		batch = &ipcBatch{}
	default:
		badIPC()
	}
	b := c.table(p, 8, 4, 4, 4, 4)
	batch.rows = int64(b.uint(0, 8))
	if batch.rows < 0 {
		badIPC()
	}
	if batch.rows > cfg.MaxRows {
		limitedIPC()
	}
	nodes, n := b.vector(1, 16, maxIPCFields)
	batch.nodes = make([][2]int64, n)
	for i := range batch.nodes {
		length, nulls := c.i64(nodes+i*16), c.i64(nodes+i*16+8)
		if length < 0 || nulls < 0 || nulls > length {
			badIPC()
		}
		if length > max(cfg.MaxRows, cfg.MaxDecodedBytes*8) {
			limitedIPC()
		}
		batch.nodes[i] = [2]int64{length, nulls}
	}
	buffers, n := b.vector(2, 16, maxIPCBuffers)
	batch.buffers = make([][2]int64, n)
	for i := range batch.buffers {
		offset, length := c.i64(buffers+i*16), c.i64(buffers+i*16+8)
		if offset < 0 || length < 0 || offset > body || length > body-offset {
			badIPC()
		}
		batch.buffers[i] = [2]int64{offset, length}
	}
	counts, n := b.vector(4, 8, maxIPCFields)
	batch.variadic = make([]int64, n)
	for i := range batch.variadic {
		batch.variadic[i] = c.i64(counts + i*8)
		if batch.variadic[i] < 0 || batch.variadic[i] > maxIPCBuffers {
			limitedIPC()
		}
	}
	if p := b.ref(3, false); p != 0 {
		compression := c.table(p, 1, 1)
		batch.compressed, batch.codec = true, byte(compression.uint(0, 1))
		if batch.codec > 1 || compression.uint(1, 1) != 0 {
			badIPC()
		}
	}
	return body, batch
}

// Rebuild only batch metadata after bounded decompression. Rewriting arbitrary
// incoming FlatBuffer offsets in place could corrupt a shared vtable.
func uncompressedMetadata(batch *ipcBatch, body int64) []byte {
	b := flatbuffers.NewBuilder(1024)
	b.StartVector(16, len(batch.nodes), 8)
	for i := len(batch.nodes) - 1; i >= 0; i-- {
		b.PrependInt64(batch.nodes[i][1])
		b.PrependInt64(batch.nodes[i][0])
	}
	nodes := b.EndVector(len(batch.nodes))
	b.StartVector(16, len(batch.buffers), 8)
	for i := len(batch.buffers) - 1; i >= 0; i-- {
		b.PrependInt64(batch.buffers[i][1])
		b.PrependInt64(batch.buffers[i][0])
	}
	buffers := b.EndVector(len(batch.buffers))
	b.StartVector(8, len(batch.variadic), 8)
	for i := len(batch.variadic) - 1; i >= 0; i-- {
		b.PrependInt64(batch.variadic[i])
	}
	variadic := b.EndVector(len(batch.variadic))
	b.StartObject(5)
	b.PrependInt64Slot(0, batch.rows, 0)
	b.PrependUOffsetTSlot(1, nodes, 0)
	b.PrependUOffsetTSlot(2, buffers, 0)
	b.PrependUOffsetTSlot(4, variadic, 0)
	header := b.EndObject()
	typ := byte(3)
	if batch.dictionary {
		b.StartObject(3)
		b.PrependInt64Slot(0, batch.id, 0)
		b.PrependUOffsetTSlot(1, header, 0)
		b.PrependBoolSlot(2, batch.delta, false)
		header, typ = b.EndObject(), 2
	}
	b.StartObject(5)
	b.PrependInt16Slot(0, 4, 0)
	b.PrependByteSlot(1, typ, 0)
	b.PrependUOffsetTSlot(2, header, 0)
	b.PrependInt64Slot(3, body, 0)
	b.Finish(b.EndObject())
	return b.FinishedBytes()
}
