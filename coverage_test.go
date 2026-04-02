package set

import (
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/tulpenhaendler/dict"
)

func TestFlushVisibility(t *testing.T) {
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

	// Index records.
	for i := 0; i < 100; i++ {
		repo.Index(uint64(i), Record{"color": "red"})
	}

	// Start a flush in a goroutine; query concurrently to check visibility.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		repo.Flush()
	}()

	// Poll until flush completes, checking that count never drops.
	// Records should always be visible (in buffer, flushing, or segment).
	for i := 0; i < 1000; i++ {
		count := repo.Count(Eq("color", "red"))
		if count < 100 {
			t.Errorf("during flush: count=%d, want >=100 (records went invisible)", count)
			break
		}
	}
	wg.Wait()

	// After flush, all records should be in the segment.
	count := repo.Count(Eq("color", "red"))
	if count != 100 {
		t.Errorf("after flush: count=%d, want 100", count)
	}
}

func TestPrefixQuery(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "data",
		Fields: []Field{
			{Name: "key", Type: Bytes(4)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"key": []byte{0x01, 0x02, 0x03, 0x04}})
	repo.Index(2, Record{"key": []byte{0x01, 0x02, 0xFF, 0xFF}})
	repo.Index(3, Record{"key": []byte{0x01, 0x03, 0x00, 0x00}})
	repo.Index(4, Record{"key": []byte{0x02, 0x00, 0x00, 0x00}})
	repo.Flush()

	// Prefix 0x0102 should match records 1 and 2.
	iter := repo.Query(Prefix("key", []byte{0x01, 0x02}))
	assertIDs(t, collectIDs(t, iter), []uint64{1, 2})

	// Prefix 0x01 should match records 1, 2, 3.
	iter = repo.Query(Prefix("key", []byte{0x01}))
	assertIDs(t, collectIDs(t, iter), []uint64{1, 2, 3})

	// Prefix 0x02 should match record 4 only.
	iter = repo.Query(Prefix("key", []byte{0x02}))
	assertIDs(t, collectIDs(t, iter), []uint64{4})

	// No match.
	iter = repo.Query(Prefix("key", []byte{0xFF}))
	assertIDs(t, collectIDs(t, iter), nil)
}

func TestBytesField(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "hashes",
		Fields: []Field{
			{Name: "hash", Type: Bytes(32)},
			{Name: "label", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	hash1 := make([]byte, 32)
	hash1[0] = 0xAA
	hash2 := make([]byte, 32)
	hash2[0] = 0xBB
	hash3 := make([]byte, 32)
	hash3[0] = 0xAA
	hash3[1] = 0x01

	repo.Index(1, Record{"hash": hash1, "label": "first"})
	repo.Index(2, Record{"hash": hash2, "label": "second"})
	repo.Index(3, Record{"hash": hash3, "label": "third"})
	repo.Flush()

	// Eq on bytes field.
	iter := repo.Query(Eq("hash", hash1))
	assertIDs(t, collectIDs(t, iter), []uint64{1})

	iter = repo.Query(Eq("hash", hash2))
	assertIDs(t, collectIDs(t, iter), []uint64{2})

	// And with bytes field.
	iter = repo.Query(And(Eq("hash", hash1), Eq("label", "first")))
	assertIDs(t, collectIDs(t, iter), []uint64{1})

	// Validate rejects wrong size.
	shortHash := make([]byte, 16)
	err = repo.schema.ValidateRecord(Record{"hash": shortHash, "label": "x"})
	if err == nil {
		t.Error("expected validation error for wrong byte size")
	}
}

func TestBigIntField(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "balances",
		Fields: []Field{
			{Name: "balance", Type: BigInt()},
			{Name: "owner", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	neg := big.NewInt(-1_000_000)
	zero := big.NewInt(0)
	small := big.NewInt(42)
	large := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1e18

	repo.Index(1, Record{"balance": neg, "owner": "alice"})
	repo.Index(2, Record{"balance": zero, "owner": "bob"})
	repo.Index(3, Record{"balance": small, "owner": "charlie"})
	repo.Index(4, Record{"balance": large, "owner": "dave"})
	repo.Flush()

	// Eq.
	iter := repo.Query(Eq("balance", zero))
	assertIDs(t, collectIDs(t, iter), []uint64{2})

	iter = repo.Query(Eq("balance", large))
	assertIDs(t, collectIDs(t, iter), []uint64{4})

	// Range: balance >= 0.
	iter = repo.Query(Gte("balance", zero))
	assertIDs(t, collectIDs(t, iter), []uint64{2, 3, 4})

	// Range: balance < 0.
	iter = repo.Query(Lt("balance", zero))
	assertIDs(t, collectIDs(t, iter), []uint64{1})

	// Between.
	iter = repo.Query(Between("balance", zero, small))
	assertIDs(t, collectIDs(t, iter), []uint64{2, 3})
}

func TestTimestampField(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "events",
		Fields: []Field{
			{Name: "ts", Type: Range(Timestamp)},
			{Name: "kind", Type: String(dict.KeyRaw)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	repo.Index(1, Record{"ts": t1, "kind": "create"})
	repo.Index(2, Record{"ts": t2, "kind": "update"})
	repo.Index(3, Record{"ts": t3, "kind": "delete"})
	repo.Flush()

	// Eq.
	iter := repo.Query(Eq("ts", t2))
	assertIDs(t, collectIDs(t, iter), []uint64{2})

	// Range: ts >= mid-2024.
	iter = repo.Query(Gte("ts", t2))
	assertIDs(t, collectIDs(t, iter), []uint64{2, 3})

	// Between.
	iter = repo.Query(Between("ts", t1, t2))
	assertIDs(t, collectIDs(t, iter), []uint64{1, 2})
}

func TestInt64Field(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "deltas",
		Fields: []Field{
			{Name: "delta", Type: Range(Int64BE)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"delta": int64(-100)})
	repo.Index(2, Record{"delta": int64(-1)})
	repo.Index(3, Record{"delta": int64(0)})
	repo.Index(4, Record{"delta": int64(50)})
	repo.Index(5, Record{"delta": int64(1000)})
	repo.Flush()

	// Negatives sort correctly.
	iter := repo.Query(Lt("delta", int64(0)))
	assertIDs(t, collectIDs(t, iter), []uint64{1, 2})

	iter = repo.Query(Gte("delta", int64(0)))
	assertIDs(t, collectIDs(t, iter), []uint64{3, 4, 5})

	iter = repo.Query(Between("delta", int64(-1), int64(50)))
	assertIDs(t, collectIDs(t, iter), []uint64{2, 3, 4})
}

func TestBoolField(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "flags",
		Fields: []Field{
			{Name: "active", Type: Bool()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"active": true})
	repo.Index(2, Record{"active": false})
	repo.Index(3, Record{"active": true})
	repo.Flush()

	iter := repo.Query(Eq("active", true))
	assertIDs(t, collectIDs(t, iter), []uint64{1, 3})

	iter = repo.Query(Eq("active", false))
	assertIDs(t, collectIDs(t, iter), []uint64{2})

	iter = repo.Query(Not(Eq("active", true)))
	assertIDs(t, collectIDs(t, iter), []uint64{2})
}

func TestEnumField(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "txs",
		Fields: []Field{
			{Name: "status", Type: Enum("applied", "failed", "backtracked")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	repo.Index(1, Record{"status": "applied"})
	repo.Index(2, Record{"status": "failed"})
	repo.Index(3, Record{"status": "applied"})
	repo.Index(4, Record{"status": "backtracked"})
	repo.Flush()

	iter := repo.Query(Eq("status", "applied"))
	assertIDs(t, collectIDs(t, iter), []uint64{1, 3})

	iter = repo.Query(Eq("status", "failed"))
	assertIDs(t, collectIDs(t, iter), []uint64{2})

	iter = repo.Query(Not(Eq("status", "applied")))
	assertIDs(t, collectIDs(t, iter), []uint64{2, 4})

	// Invalid enum value should fail validation.
	err = repo.schema.ValidateRecord(Record{"status": "unknown"})
	if err == nil {
		t.Error("expected validation error for invalid enum value")
	}
}
