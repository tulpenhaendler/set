// Package fst implements a minimal finite state transducer optimized for
// mapping sorted byte-slice keys to uint64 values. It supports exact lookup
// and ordered iteration (range scans).
//
// The FST is built from pre-sorted key/value pairs and serialized to a
// compact binary format. The on-disk format is designed for mmap'd reads.
//
// Binary format:
//
//	[magic:4]["FST2"]
//	[version:1][0x02]
//	[root_addr:8]
//	[num_keys:8]
//	[...states...]
//
// Each state:
//
//	[num_transitions:2]
//	[is_final:1]
//	[final_output:uvarint] (only if is_final)
//	for each transition:
//	  [byte:1][output:uvarint][target_addr:uvarint]
package fst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

var magic = [4]byte{'F', 'S', 'T', '2'}

const version = 2
const headerSize = 4 + 1 + 8 + 8 // magic + version + root_addr + num_keys

// Builder constructs an FST from sorted key/value pairs.
type Builder struct {
	w       io.Writer
	buf     bytes.Buffer
	written int64

	// For minimization: registry of frozen states.
	registry  map[string]int64 // serialized state → address
	scratch   bytes.Buffer     // reused for node serialization
	tmp       [binary.MaxVarintLen64]byte
	freeNodes []*builderNode // recycled nodes

	// Build stack.
	frontier []*builderNode
	lastKey  []byte
	numKeys  uint64
}

type builderNode struct {
	transitions []builderTransition
	isFinal     bool
	finalOutput uint64
}

type builderTransition struct {
	b      byte
	output uint64
	target *builderNode
	addr   int64 // address once compiled, -1 if not yet
}

// NewBuilder creates a new FST builder that writes to w.
func NewBuilder(w io.Writer) *Builder {
	b := &Builder{
		w:        w,
		registry: make(map[string]int64),
	}
	// Write header placeholder (will be patched at finish).
	b.buf.Write(magic[:])
	b.buf.WriteByte(version)
	binary.Write(&b.buf, binary.LittleEndian, uint64(0)) // root_addr placeholder
	binary.Write(&b.buf, binary.LittleEndian, uint64(0)) // num_keys placeholder
	b.written = headerSize

	b.frontier = append(b.frontier, b.getNode())
	return b
}

// Add adds a key/value pair. Keys must be added in sorted order.
func (b *Builder) Add(key []byte, value uint64) error {
	if b.lastKey != nil && bytes.Compare(key, b.lastKey) <= 0 {
		return fmt.Errorf("fst: keys must be added in sorted order (got %x after %x)", key, b.lastKey)
	}

	prefixLen := 0
	if b.lastKey != nil {
		for prefixLen < len(key) && prefixLen < len(b.lastKey) && key[prefixLen] == b.lastKey[prefixLen] {
			prefixLen++
		}
	}

	b.freezeFrom(prefixLen + 1)

	for i := len(b.frontier); i <= len(key); i++ {
		b.frontier = append(b.frontier, b.getNode())
	}

	for i := prefixLen; i < len(key); i++ {
		b.frontier[i].addTransition(key[i], 0, b.frontier[i+1])
	}

	b.frontier[len(key)].isFinal = true
	b.frontier[len(key)].finalOutput = value

	b.lastKey = append(b.lastKey[:0], key...)
	b.numKeys++
	return nil
}

// Finish completes the FST and writes remaining data.
func (b *Builder) Finish() (int64, error) {
	b.freezeFrom(1)
	rootAddr := b.compileNode(b.frontier[0])

	data := b.buf.Bytes()
	binary.LittleEndian.PutUint64(data[5:13], uint64(rootAddr))
	binary.LittleEndian.PutUint64(data[13:21], b.numKeys)

	n, err := b.w.Write(data)
	return int64(n), err
}

func (b *Builder) freezeFrom(level int) {
	for i := len(b.frontier) - 1; i >= level; i-- {
		if i > 0 {
			parent := b.frontier[i-1]
			for j := range parent.transitions {
				if parent.transitions[j].target == b.frontier[i] {
					parent.transitions[j].addr = b.compileNode(b.frontier[i])
					parent.transitions[j].target = nil
					break
				}
			}
		}
		b.putNode(b.frontier[i])
		b.frontier[i] = b.getNode()
	}
	b.frontier = b.frontier[:level]
	if level == 0 {
		if len(b.frontier) == 0 {
			b.frontier = append(b.frontier, b.getNode())
		}
	}
}

func (b *Builder) getNode() *builderNode {
	if n := len(b.freeNodes); n > 0 {
		node := b.freeNodes[n-1]
		b.freeNodes = b.freeNodes[:n-1]
		return node
	}
	return &builderNode{}
}

func (b *Builder) putNode(n *builderNode) {
	n.transitions = n.transitions[:0]
	n.isFinal = false
	n.finalOutput = 0
	b.freeNodes = append(b.freeNodes, n)
}

func (b *Builder) putUvarint(buf *bytes.Buffer, v uint64) {
	n := binary.PutUvarint(b.tmp[:], v)
	buf.Write(b.tmp[:n])
}

func (b *Builder) compileNode(n *builderNode) int64 {
	b.scratch.Reset()

	sort.Slice(n.transitions, func(i, j int) bool {
		return n.transitions[i].b < n.transitions[j].b
	})

	binary.Write(&b.scratch, binary.LittleEndian, uint16(len(n.transitions)))
	if n.isFinal {
		b.scratch.WriteByte(1)
		b.putUvarint(&b.scratch, n.finalOutput)
	} else {
		b.scratch.WriteByte(0)
	}
	for _, t := range n.transitions {
		b.scratch.WriteByte(t.b)
		b.putUvarint(&b.scratch, t.output)
		b.putUvarint(&b.scratch, uint64(t.addr))
	}

	serialized := b.scratch.Bytes()
	if addr, ok := b.registry[string(serialized)]; ok {
		return addr
	}

	addr := b.written
	b.registry[string(serialized)] = addr
	b.buf.Write(serialized)
	b.written = headerSize + int64(b.buf.Len()-int(headerSize))
	return addr
}

func (n *builderNode) addTransition(b byte, output uint64, target *builderNode) {
	n.transitions = append(n.transitions, builderTransition{
		b:      b,
		output: output,
		target: target,
		addr:   -1,
	})
}
