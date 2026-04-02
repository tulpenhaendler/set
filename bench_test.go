package set

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tulpenhaendler/dict"
)

func BenchmarkIndex(b *testing.B) {
	store, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	repo, err := store.Repo(Schema{
		Name: "txs",
		Fields: []Field{
			{Name: "from", Type: String(dict.KeyRaw)},
			{Name: "to", Type: String(dict.KeyRaw)},
			{Name: "block", Type: Range(Uint64BE)},
			{Name: "success", Type: Bool()},
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		repo.Index(uint64(i), Record{
			"from":    fmt.Sprintf("addr_%d", i%1000),
			"to":      fmt.Sprintf("addr_%d", (i+500)%1000),
			"block":   uint64(i / 100),
			"success": i%10 != 0,
		})
	}
}

func BenchmarkFlush(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				dir := filepath.Join(b.TempDir(), fmt.Sprintf("run%d", iter))
				os.MkdirAll(dir, 0755)
				store, _ := Open(dir)
				repo, _ := store.Repo(Schema{
					Name: "txs",
					Fields: []Field{
						{Name: "from", Type: String(dict.KeyRaw)},
						{Name: "to", Type: String(dict.KeyRaw)},
						{Name: "block", Type: Range(Uint64BE)},
					},
				})
				for i := 0; i < n; i++ {
					repo.Index(uint64(i), Record{
						"from":  fmt.Sprintf("addr_%d", i%1000),
						"to":    fmt.Sprintf("addr_%d", (i+500)%1000),
						"block": uint64(i / 100),
					})
				}
				b.StartTimer()

				repo.Flush()

				b.StopTimer()
				store.Close()
			}
		})
	}
}

func BenchmarkQueryEq(b *testing.B) {
	store, _ := Open(b.TempDir())
	defer store.Close()

	repo, _ := store.Repo(Schema{
		Name: "txs",
		Fields: []Field{
			{Name: "from", Type: String(dict.KeyRaw)},
			{Name: "to", Type: String(dict.KeyRaw)},
			{Name: "block", Type: Range(Uint64BE)},
		},
	})

	for i := 0; i < 100000; i++ {
		repo.Index(uint64(i), Record{
			"from":  fmt.Sprintf("addr_%d", i%1000),
			"to":    fmt.Sprintf("addr_%d", (i+500)%1000),
			"block": uint64(i / 100),
		})
	}
	repo.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := repo.Query(Eq("from", "addr_42"))
		for iter.Next() {
			_ = iter.ID()
		}
		iter.Close()
	}
}

func BenchmarkQueryAnd(b *testing.B) {
	store, _ := Open(b.TempDir())
	defer store.Close()

	repo, _ := store.Repo(Schema{
		Name: "txs",
		Fields: []Field{
			{Name: "from", Type: String(dict.KeyRaw)},
			{Name: "to", Type: String(dict.KeyRaw)},
			{Name: "block", Type: Range(Uint64BE)},
		},
	})

	for i := 0; i < 100000; i++ {
		repo.Index(uint64(i), Record{
			"from":  fmt.Sprintf("addr_%d", i%1000),
			"to":    fmt.Sprintf("addr_%d", (i+500)%1000),
			"block": uint64(i / 100),
		})
	}
	repo.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := repo.Query(And(
			Eq("from", "addr_42"),
			Eq("to", "addr_542"),
		))
		for iter.Next() {
			_ = iter.ID()
		}
		iter.Close()
	}
}

func BenchmarkQueryRange(b *testing.B) {
	store, _ := Open(b.TempDir())
	defer store.Close()

	repo, _ := store.Repo(Schema{
		Name: "txs",
		Fields: []Field{
			{Name: "from", Type: String(dict.KeyRaw)},
			{Name: "block", Type: Range(Uint64BE)},
		},
	})

	for i := 0; i < 100000; i++ {
		repo.Index(uint64(i), Record{
			"from":  fmt.Sprintf("addr_%d", i%1000),
			"block": uint64(i / 100),
		})
	}
	repo.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := repo.Query(Between("block", uint64(500), uint64(600)))
		for iter.Next() {
			_ = iter.ID()
		}
		iter.Close()
	}
}
