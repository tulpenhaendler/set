package set

import (
	"fmt"
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

	// Reopen: corrupted segment should be detected by checksum and skipped.
	store2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	repo2, err := store2.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupted segment was skipped, so no data should be visible.
	count := repo2.Count(Eq("color", "red"))
	if count != 0 {
		t.Errorf("expected 0 results after corruption, got %d", count)
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

	// Reopen: corrupted segment detected by checksum, skipped.
	store2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	repo2, err := store2.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupted segment was skipped, query should not panic.
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

func TestTieredCompaction(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	schema := Schema{
		Name: "items",
		Fields: []Field{
			{Name: "color", Type: String(dict.KeyRaw)},
			{Name: "size", Type: Range(Uint64BE)},
		},
	}
	repo, err := store.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	segCount := func() int {
		repo.mu.RLock()
		defer repo.mu.RUnlock()
		return len(repo.segments)
	}

	// Create 8 small segments (1 record each = tier 0).
	for i := 0; i < 8; i++ {
		repo.Index(uint64(i), Record{
			"color": fmt.Sprintf("color_%d", i%4),
			"size":  uint64(i),
		})
		repo.Flush()
	}

	if n := segCount(); n != 8 {
		t.Fatalf("expected 8 segments before compact, got %d", n)
	}

	// Compact: 8 tier-0 segments → should merge 4, then possibly merge again.
	if err := repo.Compact(); err != nil {
		t.Fatal(err)
	}

	n := segCount()
	if n >= 8 {
		t.Errorf("expected fewer than 8 segments after compact, got %d", n)
	}
	t.Logf("segments after first compact: %d", n)

	// Verify data integrity.
	for i := 0; i < 8; i++ {
		count := repo.Count(Eq("size", uint64(i)))
		if count != 1 {
			t.Errorf("Eq(size=%d): got %d, want 1", i, count)
		}
	}
	total := repo.Count(Gte("size", uint64(0)))
	if total != 8 {
		t.Errorf("total count = %d, want 8", total)
	}

	// Add more small segments and compact again.
	for i := 8; i < 16; i++ {
		repo.Index(uint64(i), Record{
			"color": fmt.Sprintf("color_%d", i%4),
			"size":  uint64(i),
		})
		repo.Flush()
	}

	if err := repo.Compact(); err != nil {
		t.Fatal(err)
	}

	n2 := segCount()
	t.Logf("segments after second compact: %d", n2)

	// All 16 records should still be queryable.
	total = repo.Count(Gte("size", uint64(0)))
	if total != 16 {
		t.Errorf("total count = %d, want 16", total)
	}

	// Not query should still work across compacted segments.
	iter := repo.Query(Not(Eq("color", "color_0")))
	ids := collectIDs(t, iter)
	for _, id := range ids {
		if id%4 == 0 {
			t.Errorf("Not(color_0) returned id %d which should be color_0", id)
		}
	}
}

func TestTieredCompactionPreservesLargeSegments(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "items",
		Fields: []Field{
			{Name: "val", Type: Range(Uint64BE)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create 1 large segment (100 records = tier 3).
	for i := 0; i < 100; i++ {
		repo.Index(uint64(i), Record{"val": uint64(i)})
	}
	repo.Flush()

	// Create 3 small segments (1 record each = tier 0).
	for i := 100; i < 103; i++ {
		repo.Index(uint64(i), Record{"val": uint64(i)})
		repo.Flush()
	}

	// 4 segments total but only 3 in tier 0 (below threshold).
	// The large segment should not be touched.
	if err := repo.Compact(); err != nil {
		t.Fatal(err)
	}

	repo.mu.RLock()
	n := len(repo.segments)
	repo.mu.RUnlock()

	// Should still have 4 segments (no tier reached minMerge).
	if n != 4 {
		t.Errorf("expected 4 segments (no merge), got %d", n)
	}

	total := repo.Count(Gte("val", uint64(0)))
	if total != 103 {
		t.Errorf("total = %d, want 103", total)
	}
}
