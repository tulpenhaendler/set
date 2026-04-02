package set

import (
	"fmt"
	"sync"
	"testing"

	"github.com/tulpenhaendler/dict"
)

func TestConcurrentIndexAndQuery(t *testing.T) {
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
			{Name: "block", Type: Range(Uint64BE)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Seed some data and flush so queries have something to hit.
	for i := 0; i < 1000; i++ {
		repo.Index(uint64(i), Record{
			"sender": fmt.Sprintf("addr_%d", i%50),
			"target": fmt.Sprintf("addr_%d", (i+25)%50),
			"block":  uint64(i / 10),
		})
	}
	if err := repo.Flush(); err != nil {
		t.Fatal(err)
	}

	// Hammer with concurrent writers and readers.
	var wg sync.WaitGroup
	const writers = 4
	const readers = 8
	const opsPerWorker = 500

	// Writers: Index records concurrently.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			base := uint64(1000 + w*opsPerWorker)
			for i := 0; i < opsPerWorker; i++ {
				id := base + uint64(i)
				err := repo.Index(id, Record{
					"sender": fmt.Sprintf("addr_%d", id%50),
					"target": fmt.Sprintf("addr_%d", (id+25)%50),
					"block":  uint64(id / 10),
				})
				if err != nil {
					t.Errorf("writer %d: Index(%d): %v", w, id, err)
					return
				}
			}
		}(w)
	}

	// Readers: Query concurrently.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				addr := fmt.Sprintf("addr_%d", (r+i)%50)
				iter := repo.Query(Eq("sender", addr))
				for iter.Next() {
					_ = iter.ID()
				}
				iter.Close()

				_ = repo.Count(Gte("block", uint64(i%100)))
			}
		}(r)
	}

	wg.Wait()

	// Flush and verify no corruption.
	if err := repo.Flush(); err != nil {
		t.Fatal(err)
	}

	count := repo.Count(Gte("block", uint64(0)))
	expected := uint64(1000 + writers*opsPerWorker)
	if count != expected {
		t.Errorf("total count = %d, want %d", count, expected)
	}
}

func TestConcurrentFlushAndQuery(t *testing.T) {
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

	var wg sync.WaitGroup
	const batches = 10
	const batchSize = 200

	// Writer: Index batches and flush.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := 0; b < batches; b++ {
			for i := 0; i < batchSize; i++ {
				id := uint64(b*batchSize + i)
				repo.Index(id, Record{
					"color": fmt.Sprintf("color_%d", id%10),
					"size":  uint64(id),
				})
			}
			if err := repo.Flush(); err != nil {
				t.Errorf("flush batch %d: %v", b, err)
				return
			}
		}
	}()

	// Readers: Query during flushes.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < batches*batchSize; i++ {
				color := fmt.Sprintf("color_%d", (r+i)%10)
				iter := repo.Query(Eq("color", color))
				for iter.Next() {
					_ = iter.ID()
				}
				iter.Close()
			}
		}(r)
	}

	wg.Wait()

	total := repo.Count(Gte("size", uint64(0)))
	expected := uint64(batches * batchSize)
	if total != expected {
		t.Errorf("total = %d, want %d", total, expected)
	}
}
