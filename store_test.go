package set

import (
	"testing"

	"github.com/tulpenhaendler/dict"
)

func TestStoreIntegration(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	schema := Schema{
		Name: "transactions",
		Fields: []Field{
			{Name: "from", Type: String(dict.KeyRaw)},
			{Name: "to", Type: String(dict.KeyRaw)},
			{Name: "block", Type: Range(Uint64BE)},
		},
	}

	repo, err := store.Repo(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Index some records.
	if err := repo.Index(1, Record{"from": "alice", "to": "bob", "block": uint64(100)}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Index(2, Record{"from": "alice", "to": "charlie", "block": uint64(200)}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Index(3, Record{"from": "dave", "to": "bob", "block": uint64(300)}); err != nil {
		t.Fatal(err)
	}

	// Query before flush — should scan buffer.
	t.Run("BufferQuery", func(t *testing.T) {
		iter := repo.Query(Eq("from", "alice"))
		ids := collectIDs(t, iter)
		assertIDs(t, ids, []uint64{1, 2})
	})

	// Flush and query.
	if err := repo.Flush(); err != nil {
		t.Fatal(err)
	}

	t.Run("Eq", func(t *testing.T) {
		iter := repo.Query(Eq("from", "alice"))
		assertIDs(t, collectIDs(t, iter), []uint64{1, 2})
	})

	t.Run("EqTo", func(t *testing.T) {
		iter := repo.Query(Eq("to", "bob"))
		assertIDs(t, collectIDs(t, iter), []uint64{1, 3})
	})

	t.Run("And", func(t *testing.T) {
		iter := repo.Query(And(Eq("from", "alice"), Eq("to", "bob")))
		assertIDs(t, collectIDs(t, iter), []uint64{1})
	})

	t.Run("Or", func(t *testing.T) {
		iter := repo.Query(Or(Eq("from", "alice"), Eq("from", "dave")))
		assertIDs(t, collectIDs(t, iter), []uint64{1, 2, 3})
	})

	t.Run("RangeGte", func(t *testing.T) {
		iter := repo.Query(Gte("block", uint64(200)))
		assertIDs(t, collectIDs(t, iter), []uint64{2, 3})
	})

	t.Run("RangeLt", func(t *testing.T) {
		iter := repo.Query(Lt("block", uint64(200)))
		assertIDs(t, collectIDs(t, iter), []uint64{1})
	})

	t.Run("RangeBetween", func(t *testing.T) {
		iter := repo.Query(Between("block", uint64(100), uint64(200)))
		assertIDs(t, collectIDs(t, iter), []uint64{1, 2})
	})

	t.Run("AndWithRange", func(t *testing.T) {
		iter := repo.Query(And(
			Eq("from", "alice"),
			Gte("block", uint64(200)),
		))
		assertIDs(t, collectIDs(t, iter), []uint64{2})
	})

	t.Run("Count", func(t *testing.T) {
		count := repo.Count(Eq("from", "alice"))
		if count != 2 {
			t.Errorf("Count(from=alice) = %d, want 2", count)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		iter := repo.Query(Eq("from", "nobody"))
		assertIDs(t, collectIDs(t, iter), nil)
	})

	// Reopen the store and verify persistence.
	t.Run("Persistence", func(t *testing.T) {
		store.Close()

		store2, err := Open(store.dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store2.Close()

		repo2, err := store2.Repo(schema)
		if err != nil {
			t.Fatal(err)
		}

		iter := repo2.Query(Eq("from", "alice"))
		assertIDs(t, collectIDs(t, iter), []uint64{1, 2})
	})
}

func TestCrossRepoIntersect(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	txRepo, err := store.Repo(Schema{
		Name: "transactions",
		Fields: []Field{
			{Name: "from", Type: String(dict.KeyRaw)},
			{Name: "to", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	tagRepo, err := store.Repo(Schema{
		Name: "tags",
		Fields: []Field{
			{Name: "tag", Type: String(dict.KeyRaw)},
			{Name: "project", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Use dict IDs as shared record IDs (simulating txhash dict_ids).
	txHash1, _ := store.Dict().Get("0xabc", dict.KeyRaw)
	txHash2, _ := store.Dict().Get("0xdef", dict.KeyRaw)
	txHash3, _ := store.Dict().Get("0x123", dict.KeyRaw)

	txRepo.Index(txHash1, Record{"from": "alice", "to": "bob"})
	txRepo.Index(txHash2, Record{"from": "alice", "to": "charlie"})
	txRepo.Index(txHash3, Record{"from": "dave", "to": "bob"})

	tagRepo.Index(txHash1, Record{"tag": "defi", "project": "uniswap"})
	tagRepo.Index(txHash2, Record{"tag": "nft", "project": "opensea"})
	tagRepo.Index(txHash3, Record{"tag": "defi", "project": "aave"})

	txRepo.Flush()
	tagRepo.Flush()

	// Cross-repo: from=alice AND tag=defi → txHash1
	a := txRepo.Query(Eq("from", "alice"))
	b := tagRepo.Query(Eq("tag", "defi"))
	iter := store.Intersect(a, b)

	ids := collectIDs(t, iter)
	if len(ids) != 1 || ids[0] != txHash1 {
		t.Errorf("cross-repo intersect: got %v, want [%d]", ids, txHash1)
	}

	// Verify we can resolve back through dict.
	s, _, err := store.Dict().Reverse(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if s != "0xabc" {
		t.Errorf("Reverse(%d) = %q, want 0xabc", ids[0], s)
	}

	// Cross-repo union: from=alice OR tag=defi → txHash1, txHash2, txHash3
	a = txRepo.Query(Eq("from", "alice"))
	b = tagRepo.Query(Eq("tag", "defi"))
	iter = store.Union(a, b)
	ids = collectIDs(t, iter)
	if len(ids) != 3 {
		t.Errorf("cross-repo union: got %v, want 3 results", ids)
	}
}

func TestSliceField(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "tagged",
		Fields: []Field{
			{Name: "tags", Type: Slice(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"tags": []string{"defi", "swap"}})
	repo.Index(2, Record{"tags": []string{"nft", "art"}})
	repo.Index(3, Record{"tags": []string{"defi", "lending"}})
	repo.Flush()

	iter := repo.Query(Eq("tags", "defi"))
	assertIDs(t, collectIDs(t, iter), []uint64{1, 3})

	iter = repo.Query(Eq("tags", "nft"))
	assertIDs(t, collectIDs(t, iter), []uint64{2})
}

func TestCompositeIndex(t *testing.T) {
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

	repo.Index(0, Record{"sender": "alice", "target": "bob", "level": uint64(100)})
	repo.Index(1, Record{"sender": "alice", "target": "charlie", "level": uint64(200)})
	repo.Index(2, Record{"sender": "alice", "target": "bob", "level": uint64(300)})
	repo.Index(3, Record{"sender": "dave", "target": "bob", "level": uint64(400)})
	repo.Flush()

	// And(sender=alice, target=bob) should use composite → {0, 2}
	iter := repo.Query(And(Eq("sender", "alice"), Eq("target", "bob")))
	assertIDs(t, collectIDs(t, iter), []uint64{0, 2})

	// And(sender=alice, target=charlie) → {1}
	iter = repo.Query(And(Eq("sender", "alice"), Eq("target", "charlie")))
	assertIDs(t, collectIDs(t, iter), []uint64{1})

	// And(sender=dave, target=bob) → {3}
	iter = repo.Query(And(Eq("sender", "dave"), Eq("target", "bob")))
	assertIDs(t, collectIDs(t, iter), []uint64{3})

	// And(sender=alice, target=nobody) → empty
	iter = repo.Query(And(Eq("sender", "alice"), Eq("target", "nobody")))
	assertIDs(t, collectIDs(t, iter), nil)

	// Single field queries still work.
	iter = repo.Query(Eq("sender", "alice"))
	assertIDs(t, collectIDs(t, iter), []uint64{0, 1, 2})
}

func TestCompaction(t *testing.T) {
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

	// Create 3 segments.
	repo.Index(1, Record{"color": "red", "size": uint64(10)})
	repo.Index(2, Record{"color": "blue", "size": uint64(20)})
	repo.Flush()

	repo.Index(3, Record{"color": "red", "size": uint64(30)})
	repo.Flush()

	repo.Index(4, Record{"color": "green", "size": uint64(10)})
	repo.Flush()

	// Verify pre-compaction.
	assertIDs(t, collectIDs(t, repo.Query(Eq("color", "red"))), []uint64{1, 3})
	assertIDs(t, collectIDs(t, repo.Query(Gte("size", uint64(20)))), []uint64{2, 3})

	// Compact.
	if err := repo.Compact(); err != nil {
		t.Fatal(err)
	}

	// Verify post-compaction — same results.
	assertIDs(t, collectIDs(t, repo.Query(Eq("color", "red"))), []uint64{1, 3})
	assertIDs(t, collectIDs(t, repo.Query(Eq("color", "blue"))), []uint64{2})
	assertIDs(t, collectIDs(t, repo.Query(Eq("color", "green"))), []uint64{4})
	assertIDs(t, collectIDs(t, repo.Query(Gte("size", uint64(20)))), []uint64{2, 3})
	assertIDs(t, collectIDs(t, repo.Query(And(
		Eq("color", "red"),
		Gte("size", uint64(20)),
	))), []uint64{3})
}

func collectIDs(t *testing.T, iter *Iterator) []uint64 {
	t.Helper()
	defer iter.Close()
	var ids []uint64
	for iter.Next() {
		ids = append(ids, iter.ID())
	}
	return ids
}

func assertIDs(t *testing.T, got, want []uint64) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if len(got) != len(want) {
		t.Errorf("got IDs %v, want %v", got, want)
		return
	}
	wantSet := make(map[uint64]bool)
	for _, id := range want {
		wantSet[id] = true
	}
	for _, id := range got {
		if !wantSet[id] {
			t.Errorf("unexpected ID %d in result %v (want %v)", id, got, want)
			return
		}
	}
}
