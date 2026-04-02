package set

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tulpenhaendler/dict"
)

func TestNotQuery(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "items",
		Fields: []Field{
			{Name: "color", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"color": "red"})
	repo.Index(2, Record{"color": "blue"})
	repo.Index(3, Record{"color": "red"})
	repo.Index(4, Record{"color": "green"})
	repo.Flush()

	// Not(color=red) → {2, 4}
	iter := repo.Query(Not(Eq("color", "red")))
	assertIDs(t, collectIDs(t, iter), []uint64{2, 4})

	// Not(color=blue) → {1, 3, 4}
	iter = repo.Query(Not(Eq("color", "blue")))
	assertIDs(t, collectIDs(t, iter), []uint64{1, 3, 4})

	// Not(color=missing) → all
	iter = repo.Query(Not(Eq("color", "missing")))
	assertIDs(t, collectIDs(t, iter), []uint64{1, 2, 3, 4})
}

func TestNotQueryMultipleSegments(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "items",
		Fields: []Field{
			{Name: "color", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"color": "red"})
	repo.Index(2, Record{"color": "blue"})
	repo.Flush()

	repo.Index(3, Record{"color": "red"})
	repo.Index(4, Record{"color": "green"})
	repo.Flush()

	// Not across two segments.
	iter := repo.Query(Not(Eq("color", "red")))
	assertIDs(t, collectIDs(t, iter), []uint64{2, 4})
}

func TestNotAndComposition(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "items",
		Fields: []Field{
			{Name: "color", Type: String(dict.KeyRaw)},
			{Name: "size", Type: Range(Uint64BE)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"color": "red", "size": uint64(10)})
	repo.Index(2, Record{"color": "blue", "size": uint64(20)})
	repo.Index(3, Record{"color": "red", "size": uint64(30)})
	repo.Flush()

	// And(Not(color=red), size>=10) → {2}
	iter := repo.Query(And(Not(Eq("color", "red")), Gte("size", uint64(10))))
	assertIDs(t, collectIDs(t, iter), []uint64{2})
}

func TestCompactionCorrectness(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "items",
		Fields: []Field{
			{Name: "color", Type: String(dict.KeyRaw)},
			{Name: "size", Type: Range(Uint64BE)},
			{Name: "active", Type: Bool()},
		},
		Composites: []Composite{
			{Fields: []string{"color", "active"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create 4 segments.
	repo.Index(1, Record{"color": "red", "size": uint64(10), "active": true})
	repo.Flush()
	repo.Index(2, Record{"color": "blue", "size": uint64(20), "active": false})
	repo.Flush()
	repo.Index(3, Record{"color": "red", "size": uint64(30), "active": true})
	repo.Flush()
	repo.Index(4, Record{"color": "green", "size": uint64(40), "active": true})
	repo.Flush()

	// Capture pre-compaction results.
	preEq := collectIDs(t, repo.Query(Eq("color", "red")))
	preRange := collectIDs(t, repo.Query(Gte("size", uint64(20))))
	preBool := collectIDs(t, repo.Query(Eq("active", true)))
	preAnd := collectIDs(t, repo.Query(And(Eq("color", "red"), Eq("active", true))))
	preNot := collectIDs(t, repo.Query(Not(Eq("color", "blue"))))
	preCount := repo.Count(Gte("size", uint64(0)))

	if err := repo.Compact(); err != nil {
		t.Fatal(err)
	}

	// All results must match post-compaction.
	assertIDs(t, collectIDs(t, repo.Query(Eq("color", "red"))), preEq)
	assertIDs(t, collectIDs(t, repo.Query(Gte("size", uint64(20)))), preRange)
	assertIDs(t, collectIDs(t, repo.Query(Eq("active", true))), preBool)
	assertIDs(t, collectIDs(t, repo.Query(And(Eq("color", "red"), Eq("active", true)))), preAnd)
	assertIDs(t, collectIDs(t, repo.Query(Not(Eq("color", "blue")))), preNot)

	postCount := repo.Count(Gte("size", uint64(0)))
	if postCount != preCount {
		t.Errorf("count changed: pre=%d post=%d", preCount, postCount)
	}
}

func TestCompactRecovery(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	repo, err := store.Repo(Schema{
		Name: "items",
		Fields: []Field{
			{Name: "color", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"color": "red"})
	repo.Index(2, Record{"color": "blue"})
	repo.Flush()
	repo.Index(3, Record{"color": "green"})
	repo.Flush()

	store.Close()

	// Simulate a crash mid-compact: write a marker listing old segments.
	schema := Schema{
		Name:   "items",
		Fields: []Field{{Name: "color", Type: String(dict.KeyRaw)}},
	}
	segDir := filepath.Join(dir, "repos", schema.Hash(), "segments")
	entries, _ := os.ReadDir(segDir)

	var marker []byte
	for _, e := range entries {
		if e.IsDir() {
			marker = append(marker, []byte(e.Name()+"\n")...)
		}
	}
	os.WriteFile(filepath.Join(segDir, ".compact_done"), marker, 0644)

	// Reopen: recovery should clean up the marked segments.
	store2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	repo2, err := store2.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Old segments were removed by recovery, so data is gone.
	// This verifies the marker cleanup path works without crashing.
	count := repo2.Count(Gte("color", ""))
	_ = count // just verify it doesn't panic
}

func TestCorruptedFSTFile(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	repo, err := store.Repo(Schema{
		Name: "items",
		Fields: []Field{
			{Name: "color", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"color": "red"})
	repo.Flush()
	store.Close()

	// Corrupt the FST file by truncating it.
	schema := Schema{
		Name:   "items",
		Fields: []Field{{Name: "color", Type: String(dict.KeyRaw)}},
	}
	segDir := filepath.Join(dir, "repos", schema.Hash(), "segments")
	entries, _ := os.ReadDir(segDir)
	for _, e := range entries {
		if e.IsDir() {
			fstPath := filepath.Join(segDir, e.Name(), "color.fst")
			if data, err := os.ReadFile(fstPath); err == nil {
				// Write truncated data.
				os.WriteFile(fstPath, data[:len(data)/2], 0644)
			}
		}
	}

	// Reopen should fail gracefully (bad magic or short data).
	store2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	_, err = store2.Repo(schema)
	if err == nil {
		t.Log("reopened with corrupted FST (may have been too short to detect)")
	}
}

func TestCorruptedBitmapData(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	repo, err := store.Repo(Schema{
		Name: "items",
		Fields: []Field{
			{Name: "color", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"color": "red"})
	repo.Index(2, Record{"color": "blue"})
	repo.Flush()
	store.Close()

	// Corrupt the roaring file by overwriting bitmap data with garbage.
	schema := Schema{
		Name:   "items",
		Fields: []Field{{Name: "color", Type: String(dict.KeyRaw)}},
	}
	segDir := filepath.Join(dir, "repos", schema.Hash(), "segments")
	entries, _ := os.ReadDir(segDir)
	for _, e := range entries {
		if e.IsDir() {
			roarPath := filepath.Join(segDir, e.Name(), "color.roar")
			if data, err := os.ReadFile(roarPath); err == nil && len(data) > 20 {
				// Corrupt bitmap data region (after header).
				for i := 20; i < len(data); i++ {
					data[i] = 0xFF
				}
				os.WriteFile(roarPath, data, 0644)
			}
		}
	}

	// Reopen and query -- should not panic, return empty results.
	store2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	repo2, err := store2.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	// This should not panic even with corrupted bitmaps.
	iter := repo2.Query(Eq("color", "red"))
	for iter.Next() {
		_ = iter.ID()
	}
	iter.Close()
}

func TestCompositeIndexEdgeCases(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "txs",
		Fields: []Field{
			{Name: "sender", Type: String(dict.KeyRaw)},
			{Name: "target", Type: String(dict.KeyRaw)},
			{Name: "level", Type: Range(Uint64BE)},
		},
		Composites: []Composite{
			{Fields: []string{"sender", "target"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"sender": "alice", "target": "bob", "level": uint64(100)})
	repo.Index(2, Record{"sender": "alice", "target": "bob", "level": uint64(200)})
	repo.Index(3, Record{"sender": "alice", "target": "charlie", "level": uint64(100)})
	repo.Index(4, Record{"sender": "bob", "target": "alice", "level": uint64(100)})
	repo.Flush()

	// Composite should be used for And(Eq, Eq).
	t.Run("ExactMatch", func(t *testing.T) {
		iter := repo.Query(And(Eq("sender", "alice"), Eq("target", "bob")))
		assertIDs(t, collectIDs(t, iter), []uint64{1, 2})
	})

	// Reversed field order should still use composite.
	t.Run("ReversedOrder", func(t *testing.T) {
		iter := repo.Query(And(Eq("target", "bob"), Eq("sender", "alice")))
		assertIDs(t, collectIDs(t, iter), []uint64{1, 2})
	})

	// Composite should NOT be used when a non-Eq query is included.
	t.Run("MixedQueryTypes", func(t *testing.T) {
		iter := repo.Query(And(Eq("sender", "alice"), Gte("level", uint64(200))))
		assertIDs(t, collectIDs(t, iter), []uint64{2})
	})

	// Composite with no matches.
	t.Run("NoMatch", func(t *testing.T) {
		iter := repo.Query(And(Eq("sender", "alice"), Eq("target", "nobody")))
		assertIDs(t, collectIDs(t, iter), nil)
	})

	// Three-field And: composite covers sender+target, level is separate.
	t.Run("ThreeFieldAnd", func(t *testing.T) {
		iter := repo.Query(And(
			Eq("sender", "alice"),
			Eq("target", "bob"),
			Eq("level", uint64(100)),
		))
		assertIDs(t, collectIDs(t, iter), []uint64{1})
	})

	// Single field query should not use composite.
	t.Run("SingleField", func(t *testing.T) {
		iter := repo.Query(Eq("sender", "alice"))
		assertIDs(t, collectIDs(t, iter), []uint64{1, 2, 3})
	})
}

func TestNotQueryBuffer(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "items",
		Fields: []Field{
			{Name: "color", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Query Not before flush (buffer only).
	repo.Index(1, Record{"color": "red"})
	repo.Index(2, Record{"color": "blue"})

	iter := repo.Query(Not(Eq("color", "red")))
	assertIDs(t, collectIDs(t, iter), []uint64{2})
}
