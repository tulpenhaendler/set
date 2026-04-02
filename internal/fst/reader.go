package fst

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// FST is a read-only finite state transducer backed by a byte slice.
type FST struct {
	data     []byte
	rootAddr int64
	numKeys  uint64
}

type state struct {
	numTrans    int
	isFinal     bool
	finalOutput uint64
	transBase   int // offset into data where transitions start
}

type transition struct {
	b      byte
	output uint64
	addr   int64
}

// Load creates an FST from a byte slice (e.g. mmap'd file).
func Load(data []byte) (*FST, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("fst: data too short")
	}
	if data[0] != magic[0] || data[1] != magic[1] || data[2] != magic[2] || data[3] != magic[3] {
		return nil, fmt.Errorf("fst: bad magic")
	}
	if data[4] != version {
		return nil, fmt.Errorf("fst: unsupported version %d", data[4])
	}
	rootAddr := int64(binary.LittleEndian.Uint64(data[5:13]))
	numKeys := binary.LittleEndian.Uint64(data[13:21])
	return &FST{data: data, rootAddr: rootAddr, numKeys: numKeys}, nil
}

// Len returns the number of keys in the FST.
func (f *FST) Len() uint64 {
	return f.numKeys
}

// Get looks up a key and returns its value and whether it was found.
func (f *FST) Get(key []byte) (uint64, bool) {
	s := f.readState(f.rootAddr)
	var output uint64

	for _, b := range key {
		t, ok := f.findTransition(s, b)
		if !ok {
			return 0, false
		}
		output += t.output
		s = f.readState(t.addr)
	}

	if !s.isFinal {
		return 0, false
	}
	return output + s.finalOutput, true
}

// Iterator returns an iterator over all keys in [from, to].
// Pass nil for from/to to indicate no bound.
func (f *FST) Iterator(from, to []byte) *Iter {
	it := &Iter{
		f:    f,
		to:   to,
		done: false,
	}
	it.seekFrom(from)
	return it
}

// IteratorPrefix returns an iterator over all keys with the given prefix.
func (f *FST) IteratorPrefix(prefix []byte) *Iter {
	it := &Iter{f: f, done: false}
	if len(prefix) > 0 {
		var to []byte
		if len(prefix) <= len(it.toBuf) {
			n := copy(it.toBuf[:], prefix)
			to = it.toBuf[:n]
		} else {
			to = make([]byte, len(prefix))
			copy(to, prefix)
		}
		for len(to) > 0 {
			if to[len(to)-1] < 0xFF {
				to[len(to)-1]++
				break
			}
			to = to[:len(to)-1]
		}
		if len(to) > 0 {
			it.to = to
		}
	}
	it.seekFrom(prefix)
	return it
}

// Iter iterates over FST entries in sorted key order.
type Iter struct {
	f     *FST
	to    []byte
	toBuf [32]byte // inline storage for short upper bounds
	stack []iterFrame
	key   []byte
	value uint64
	done  bool
}

type iterFrame struct {
	state        state
	transIdx     int    // next transition index to visit
	transOff     int    // byte offset of next transition to read
	output       uint64
	keyLen       int
	finalEmitted bool
}

func (it *Iter) seekFrom(from []byte) {
	it.stack = it.stack[:0]
	it.key = it.key[:0]

	s := it.f.readState(it.f.rootAddr)
	it.stack = append(it.stack, iterFrame{
		state:    s,
		output:   0,
		keyLen:   0,
		transOff: s.transBase,
	})

	if from == nil {
		return
	}

	for i := 0; i < len(from); i++ {
		frame := &it.stack[len(it.stack)-1]
		s := frame.state

		exactMatch := false
		anyMatch := false
		off := s.transBase
		for j := 0; j < s.numTrans; j++ {
			t, nextOff := it.f.readTransitionAt(off)
			if t.b >= from[i] {
				anyMatch = true
				if t.b == from[i] {
					it.key = append(it.key, t.b)
					nextOutput := frame.output + t.output
					nextState := it.f.readState(t.addr)
					frame.transIdx = j + 1
					frame.transOff = nextOff
					it.stack = append(it.stack, iterFrame{
						state:    nextState,
						output:   nextOutput,
						keyLen:   len(it.key),
						transOff: nextState.transBase,
					})
					exactMatch = true
				} else {
					frame.transIdx = j
					frame.transOff = off
				}
				break
			}
			off = nextOff
		}
		if !anyMatch {
			frame.transIdx = s.numTrans
			break
		}
		if !exactMatch {
			break
		}
	}

	if from != nil && len(it.stack) > 0 {
		for si := range it.stack {
			it.key = it.key[:it.stack[si].keyLen]
			if bytes.Compare(it.key, from) < 0 {
				it.stack[si].finalEmitted = true
			}
		}
		if len(it.stack) > 0 {
			it.key = it.key[:it.stack[len(it.stack)-1].keyLen]
		}
	}
}

// Next advances the iterator. Returns false when done.
func (it *Iter) Next() bool {
	if it.done {
		return false
	}

	for len(it.stack) > 0 {
		frame := &it.stack[len(it.stack)-1]

		if frame.state.isFinal && !frame.finalEmitted {
			frame.finalEmitted = true
			it.key = it.key[:frame.keyLen]
			it.value = frame.output + frame.state.finalOutput

			if it.to != nil && bytes.Compare(it.key, it.to) >= 0 {
				it.done = true
				return false
			}
			return true
		}

		if frame.transIdx < frame.state.numTrans {
			t, nextOff := it.f.readTransitionAt(frame.transOff)
			frame.transIdx++
			frame.transOff = nextOff

			it.key = append(it.key[:frame.keyLen], t.b)
			nextOutput := frame.output + t.output
			nextState := it.f.readState(t.addr)

			it.stack = append(it.stack, iterFrame{
				state:    nextState,
				transIdx: 0,
				transOff: nextState.transBase,
				output:   nextOutput,
				keyLen:   len(it.key),
			})
			continue
		}

		it.stack = it.stack[:len(it.stack)-1]
	}

	it.done = true
	return false
}

// Key returns the current key. Valid only after Next() returns true.
func (it *Iter) Key() []byte {
	return it.key
}

// Value returns the current value. Valid only after Next() returns true.
func (it *Iter) Value() uint64 {
	return it.value
}

func (f *FST) readState(addr int64) state {
	off := int(addr)
	if off+3 > len(f.data) {
		return state{}
	}
	numTrans := int(binary.LittleEndian.Uint16(f.data[off : off+2]))
	off += 2
	isFinal := f.data[off] != 0
	off++
	var finalOutput uint64
	if isFinal {
		if off >= len(f.data) {
			return state{}
		}
		var n int
		finalOutput, n = binary.Uvarint(f.data[off:])
		off += n
	}
	return state{
		numTrans:    numTrans,
		isFinal:     isFinal,
		finalOutput: finalOutput,
		transBase:   off,
	}
}

// readTransitionAt reads a transition at the given byte offset.
// Returns the transition and the offset of the next transition.
func (f *FST) readTransitionAt(off int) (transition, int) {
	if off >= len(f.data) {
		return transition{}, off
	}
	b := f.data[off]
	off++
	if off >= len(f.data) {
		return transition{b: b}, off
	}
	output, n := binary.Uvarint(f.data[off:])
	off += n
	addr, n := binary.Uvarint(f.data[off:])
	off += n
	return transition{b: b, output: output, addr: int64(addr)}, off
}

func (f *FST) findTransition(s state, b byte) (transition, bool) {
	off := s.transBase
	for i := 0; i < s.numTrans; i++ {
		t, nextOff := f.readTransitionAt(off)
		if t.b == b {
			return t, true
		}
		if t.b > b {
			break
		}
		off = nextOff
	}
	return transition{}, false
}
