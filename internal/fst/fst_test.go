package fst

import (
	"bytes"
	"testing"
)

func buildFST(t *testing.T, entries []struct {
	key   []byte
	value uint64
}) *FST {
	t.Helper()
	var buf bytes.Buffer
	b := NewBuilder(&buf)
	for _, e := range entries {
		if err := b.Add(e.key, e.value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Finish(); err != nil {
		t.Fatal(err)
	}
	f, err := Load(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestFSTGetExact(t *testing.T) {
	entries := []struct {
		key   []byte
		value uint64
	}{
		{[]byte("apple"), 1},
		{[]byte("application"), 2},
		{[]byte("banana"), 3},
		{[]byte("cherry"), 4},
	}

	f := buildFST(t, entries)

	if f.Len() != 4 {
		t.Fatalf("Len() = %d, want 4", f.Len())
	}

	for _, e := range entries {
		v, ok := f.Get(e.key)
		if !ok {
			t.Errorf("Get(%q): not found", e.key)
			continue
		}
		if v != e.value {
			t.Errorf("Get(%q) = %d, want %d", e.key, v, e.value)
		}
	}

	// Not found.
	if _, ok := f.Get([]byte("app")); ok {
		t.Error("Get(app) should not be found")
	}
	if _, ok := f.Get([]byte("bananas")); ok {
		t.Error("Get(bananas) should not be found")
	}
	if _, ok := f.Get([]byte("zzz")); ok {
		t.Error("Get(zzz) should not be found")
	}
}

func TestFSTIterateAll(t *testing.T) {
	entries := []struct {
		key   []byte
		value uint64
	}{
		{[]byte("a"), 10},
		{[]byte("ab"), 20},
		{[]byte("abc"), 30},
		{[]byte("b"), 40},
		{[]byte("bc"), 50},
	}

	f := buildFST(t, entries)
	it := f.Iterator(nil, nil)

	var got []struct {
		key   string
		value uint64
	}
	for it.Next() {
		got = append(got, struct {
			key   string
			value uint64
		}{string(it.Key()), it.Value()})
	}

	if len(got) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(got), len(entries))
	}
	for i, e := range entries {
		if got[i].key != string(e.key) || got[i].value != e.value {
			t.Errorf("entry %d: got (%q, %d), want (%q, %d)", i, got[i].key, got[i].value, e.key, e.value)
		}
	}
}

func TestFSTIterateRange(t *testing.T) {
	entries := []struct {
		key   []byte
		value uint64
	}{
		{[]byte("a"), 1},
		{[]byte("b"), 2},
		{[]byte("c"), 3},
		{[]byte("d"), 4},
		{[]byte("e"), 5},
	}

	f := buildFST(t, entries)

	// Range [b, d) — should get b, c
	it := f.Iterator([]byte("b"), []byte("d"))
	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if len(keys) != 2 || keys[0] != "b" || keys[1] != "c" {
		t.Errorf("range [b,d): got %v, want [b c]", keys)
	}

	// Range [c, nil) — should get c, d, e
	it = f.Iterator([]byte("c"), nil)
	keys = keys[:0]
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if len(keys) != 3 || keys[0] != "c" || keys[1] != "d" || keys[2] != "e" {
		t.Errorf("range [c,): got %v, want [c d e]", keys)
	}
}

func TestFSTIteratePrefix(t *testing.T) {
	entries := []struct {
		key   []byte
		value uint64
	}{
		{[]byte("bar"), 1},
		{[]byte("baz"), 2},
		{[]byte("foo"), 3},
		{[]byte("foobar"), 4},
		{[]byte("foobaz"), 5},
		{[]byte("foz"), 6},
	}

	f := buildFST(t, entries)
	it := f.IteratorPrefix([]byte("foo"))
	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if len(keys) != 3 || keys[0] != "foo" || keys[1] != "foobar" || keys[2] != "foobaz" {
		t.Errorf("prefix foo: got %v, want [foo foobar foobaz]", keys)
	}
}

func TestFSTEmpty(t *testing.T) {
	var buf bytes.Buffer
	b := NewBuilder(&buf)
	if _, err := b.Finish(); err != nil {
		t.Fatal(err)
	}
	f, err := Load(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if f.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", f.Len())
	}
	if _, ok := f.Get([]byte("anything")); ok {
		t.Error("empty FST should not find anything")
	}
	it := f.Iterator(nil, nil)
	if it.Next() {
		t.Error("empty FST iterator should return nothing")
	}
}

func TestFSTSingleKey(t *testing.T) {
	entries := []struct {
		key   []byte
		value uint64
	}{
		{[]byte("only"), 42},
	}

	f := buildFST(t, entries)
	v, ok := f.Get([]byte("only"))
	if !ok || v != 42 {
		t.Errorf("Get(only) = %d, %v; want 42, true", v, ok)
	}

	it := f.Iterator(nil, nil)
	if !it.Next() {
		t.Fatal("expected one entry")
	}
	if string(it.Key()) != "only" || it.Value() != 42 {
		t.Errorf("got (%q, %d), want (only, 42)", it.Key(), it.Value())
	}
	if it.Next() {
		t.Error("expected only one entry")
	}
}

func TestFSTUint64Keys(t *testing.T) {
	// Simulate our actual use case: varint-encoded uint64 keys.
	encode := func(v uint64) []byte {
		if v == 0 {
			return []byte{1, 0}
		}
		n := 0
		for tmp := v; tmp > 0; tmp >>= 8 {
			n++
		}
		buf := make([]byte, n+1)
		buf[0] = byte(n)
		for i := n; i > 0; i-- {
			buf[i] = byte(v)
			v >>= 8
		}
		return buf
	}

	entries := []struct {
		key   []byte
		value uint64
	}{
		{encode(0), 100},
		{encode(1), 101},
		{encode(42), 102},
		{encode(256), 103},
		{encode(70000), 104},
	}

	f := buildFST(t, entries)

	for _, e := range entries {
		v, ok := f.Get(e.key)
		if !ok || v != e.value {
			t.Errorf("Get(%x) = %d, %v; want %d, true", e.key, v, ok, e.value)
		}
	}

	// Range: keys for values 42..70000
	it := f.Iterator(encode(42), nil)
	var vals []uint64
	for it.Next() {
		vals = append(vals, it.Value())
	}
	if len(vals) != 3 || vals[0] != 102 || vals[1] != 103 || vals[2] != 104 {
		t.Errorf("range [42,): got values %v, want [102 103 104]", vals)
	}
}

func TestFSTSortOrderViolation(t *testing.T) {
	var buf bytes.Buffer
	b := NewBuilder(&buf)
	b.Add([]byte("b"), 1)
	err := b.Add([]byte("a"), 2)
	if err == nil {
		t.Error("expected error for sort order violation")
	}
}
