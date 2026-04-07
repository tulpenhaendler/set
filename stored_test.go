package set

import (
	"testing"

	"github.com/tulpenhaendler/dict"
)

func TestStoredField(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	schema := Schema{
		Name: "transactions",
		Fields: []Field{
			{Name: "sender", Type: String(dict.KeyRaw)},
			{Name: "level", Type: Range(Uint64BE)},
			{Name: "op_index", Type: Stored(Uint64BE)},
			{Name: "op_hash", Type: Stored(dict.KeyRaw)},
		},
	}

	repo, err := store.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.Index(1, Record{
		"sender":   "alice",
		"level":    uint64(100),
		"op_index": uint64(0),
		"op_hash":  "oo1abc",
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Index(2, Record{
		"sender":   "bob",
		"level":    uint64(200),
		"op_index": uint64(3),
		"op_hash":  "oo2def",
	}); err != nil {
		t.Fatal(err)
	}

	// Test stored values from in-memory buffer.
	t.Run("Buffer", func(t *testing.T) {
		iter := repo.Query(Eq("sender", "bob"))
		if !iter.Next() {
			t.Fatal("expected result")
		}
		if iter.ID() != 2 {
			t.Fatalf("expected id 2, got %d", iter.ID())
		}
		if v := iter.Stored("op_index"); v != uint64(3) {
			t.Fatalf("expected op_index=3, got %v (%T)", v, v)
		}
		if v := iter.Stored("op_hash"); v != "oo2def" {
			t.Fatalf("expected op_hash=oo2def, got %v", v)
		}
		// Non-stored field returns nil.
		if v := iter.Stored("sender"); v != nil {
			t.Fatalf("expected nil for non-stored field, got %v", v)
		}
		iter.Close()
	})

	// Flush and test stored values from disk.
	if err := repo.Flush(); err != nil {
		t.Fatal(err)
	}

	t.Run("AfterFlush", func(t *testing.T) {
		iter := repo.Query(Eq("sender", "alice"))
		if !iter.Next() {
			t.Fatal("expected result")
		}
		if v := iter.Stored("op_index"); v != uint64(0) {
			t.Fatalf("expected op_index=0, got %v (%T)", v, v)
		}
		if v := iter.Stored("op_hash"); v != "oo1abc" {
			t.Fatalf("expected op_hash=oo1abc, got %v", v)
		}
		iter.Close()
	})

	// Test range query still works with mixed stored/indexed fields.
	t.Run("RangeQuery", func(t *testing.T) {
		iter := repo.Query(Gte("level", uint64(100)))
		ids := collectIDs(t, iter)
		assertIDs(t, ids, []uint64{1, 2})
	})
}

func TestStoredFieldCompaction(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	schema := Schema{
		Name: "txs",
		Fields: []Field{
			{Name: "sender", Type: String(dict.KeyRaw)},
			{Name: "amount", Type: Stored(Uint64BE)},
		},
	}

	repo, err := store.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Create 4 segments (compactMinMerge = 4).
	for seg := 0; seg < 4; seg++ {
		id := uint64(seg)
		if err := repo.Index(id, Record{
			"sender": "alice",
			"amount": uint64(100 + id),
		}); err != nil {
			t.Fatal(err)
		}
		if err := repo.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// Compact.
	if err := repo.Compact(); err != nil {
		t.Fatal(err)
	}

	// Verify stored values survived compaction.
	iter := repo.Query(Eq("sender", "alice"))
	found := map[uint64]uint64{}
	for iter.Next() {
		v := iter.Stored("amount")
		if v == nil {
			t.Fatalf("stored value nil for id %d", iter.ID())
		}
		found[iter.ID()] = v.(uint64)
	}
	iter.Close()

	for id := uint64(0); id < 4; id++ {
		want := uint64(100 + id)
		if got, ok := found[id]; !ok {
			t.Fatalf("missing id %d", id)
		} else if got != want {
			t.Fatalf("id %d: expected amount=%d, got %d", id, want, got)
		}
	}
}

func TestStoredFieldValidation(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	schema := Schema{
		Name: "test",
		Fields: []Field{
			{Name: "key", Type: String(dict.KeyRaw)},
			{Name: "val", Type: Stored(Uint64BE)},
		},
	}

	repo, err := store.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Wrong type should fail validation.
	err = repo.Index(1, Record{"key": "a", "val": "not-a-uint64"})
	if err == nil {
		t.Fatal("expected validation error for wrong type")
	}
}

func TestStoredFieldCrossRepoIterator(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	schema := Schema{
		Name: "txs",
		Fields: []Field{
			{Name: "sender", Type: String(dict.KeyRaw)},
			{Name: "amount", Type: Stored(Uint64BE)},
		},
	}

	repo, err := store.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.Index(1, Record{"sender": "alice", "amount": uint64(50)}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Flush(); err != nil {
		t.Fatal(err)
	}

	// Cross-repo iterator (Intersect) should not support Stored().
	iter1 := repo.Query(Eq("sender", "alice"))
	iter2 := repo.Query(Eq("sender", "alice"))
	cross := store.Intersect(iter1, iter2)
	if cross.Next() {
		if v := cross.Stored("amount"); v != nil {
			t.Fatalf("expected nil from cross-repo Stored(), got %v", v)
		}
	}
	cross.Close()
}

func TestStoredFieldInt64(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	schema := Schema{
		Name: "test",
		Fields: []Field{
			{Name: "key", Type: String(dict.KeyRaw)},
			{Name: "val", Type: Stored(Int64BE)},
		},
	}

	repo, err := store.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.Index(1, Record{"key": "a", "val": int64(-42)}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Flush(); err != nil {
		t.Fatal(err)
	}

	iter := repo.Query(Eq("key", "a"))
	if !iter.Next() {
		t.Fatal("expected result")
	}
	if v := iter.Stored("val"); v != int64(-42) {
		t.Fatalf("expected -42, got %v (%T)", v, v)
	}
	iter.Close()
}
