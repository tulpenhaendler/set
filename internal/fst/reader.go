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
	// Seek to the first key >= from.
	it.seekFrom(from)
	return it
}

// IteratorPrefix returns an iterator over all keys with the given prefix.
func (f *FST) IteratorPrefix(prefix []byte) *Iter {
	// Compute upper bound: prefix with last byte incremented.
	// If the prefix ends with 0xFF bytes, we strip them (no upper bound for that suffix).
	var to []byte
	if len(prefix) > 0 {
		to = make([]byte, len(prefix))
		copy(to, prefix)
		for len(to) > 0 {
			if to[len(to)-1] < 0xFF {
				to[len(to)-1]++
				break
			}
			to = to[:len(to)-1]
		}
		if len(to) == 0 {
			to = nil // prefix is all 0xFF, no upper bound
		}
	}
	return f.Iterator(prefix, to)
}

// Iter iterates over FST entries in sorted key order.
type Iter struct {
	f    *FST
	to   []byte
	stack []iterFrame
	key  []byte
	value uint64
	done bool
}

type iterFrame struct {
	state        state
	transIdx     int // next transition index to visit
	output       uint64
	keyLen       int  // length of key at this frame
	finalEmitted bool // whether we've already emitted this state's final output
}

func (it *Iter) seekFrom(from []byte) {
	it.stack = it.stack[:0]
	it.key = it.key[:0]

	s := it.f.readState(it.f.rootAddr)
	it.stack = append(it.stack, iterFrame{
		state:  s,
		output: 0,
		keyLen: 0,
	})

	if from == nil {
		return
	}

	// Walk down the FST following `from`, setting transIdx to skip earlier transitions.
	for i := 0; i < len(from); i++ {
		frame := &it.stack[len(it.stack)-1]
		s := frame.state

		// Find the first transition >= from[i].
		exactMatch := false
		anyMatch := false
		for j := 0; j < s.numTrans; j++ {
			t := it.f.readTransition(s, j)
			if t.b >= from[i] {
				anyMatch = true
				if t.b == from[i] {
					// Exact match: follow this transition and continue seeking.
					it.key = append(it.key, t.b)
					nextOutput := frame.output + t.output
					nextState := it.f.readState(t.addr)
					frame.transIdx = j + 1 // parent will continue from next transition
					it.stack = append(it.stack, iterFrame{
						state:  nextState,
						output: nextOutput,
						keyLen: len(it.key),
					})
					exactMatch = true
				} else {
					// t.b > from[i]: all keys from this transition onward are >= from.
					// Stop seeking deeper; iteration will start from this transition.
					frame.transIdx = j
				}
				break
			}
		}
		if !anyMatch {
			// All transitions < from[i]. This frame will pop (no more transitions).
			frame.transIdx = s.numTrans
			break
		}
		if !exactMatch {
			// Found a greater transition; stop seeking deeper.
			break
		}
	}

	// Mark final outputs as emitted for states that correspond to keys < from.
	// The deepest frame on the stack is at the seek position — if the key
	// built so far is less than from, we should skip its final output.
	if from != nil && len(it.stack) > 0 {
		for si := range it.stack {
			it.key = it.key[:it.stack[si].keyLen]
			if bytes.Compare(it.key, from) < 0 {
				it.stack[si].finalEmitted = true
			}
		}
		// Restore key to the deepest frame's key.
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

		// Check if this state is final and we haven't emitted it yet.
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

		// Try next transition.
		if frame.transIdx < frame.state.numTrans {
			t := it.f.readTransition(frame.state, frame.transIdx)
			frame.transIdx++

			it.key = append(it.key[:frame.keyLen], t.b)
			nextOutput := frame.output + t.output
			nextState := it.f.readState(t.addr)

			it.stack = append(it.stack, iterFrame{
				state:    nextState,
				transIdx: 0,
				output:   nextOutput,
				keyLen:   len(it.key),
			})
			continue
		}

		// Pop frame.
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
	numTrans := int(binary.LittleEndian.Uint16(f.data[off : off+2]))
	off += 2
	isFinal := f.data[off] != 0
	off++
	var finalOutput uint64
	if isFinal {
		finalOutput = binary.LittleEndian.Uint64(f.data[off : off+8])
		off += 8
	}
	return state{
		numTrans:    numTrans,
		isFinal:     isFinal,
		finalOutput: finalOutput,
		transBase:   off,
	}
}

func (f *FST) readTransition(s state, idx int) transition {
	// Each transition: [byte:1][output:8][addr:8] = 17 bytes
	off := s.transBase + idx*17
	return transition{
		b:      f.data[off],
		output: binary.LittleEndian.Uint64(f.data[off+1 : off+9]),
		addr:   int64(binary.LittleEndian.Uint64(f.data[off+9 : off+17])),
	}
}

func (f *FST) findTransition(s state, b byte) (transition, bool) {
	// Linear scan (states usually have few transitions).
	for i := 0; i < s.numTrans; i++ {
		t := f.readTransition(s, i)
		if t.b == b {
			return t, true
		}
		if t.b > b {
			break // transitions are sorted
		}
	}
	return transition{}, false
}

