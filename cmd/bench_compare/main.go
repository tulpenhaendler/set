// Benchmarks FST index vs SQLite on 500k real Tezos transactions.
//
// Compares: index/insert time, disk size, and query latency for
// equality, intersection, range, and combined queries.
//
// Usage: go run ./cmd/bench_compare
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/tulpenhaendler/dict"
	"github.com/tulpenhaendler/set"
)

type Transaction struct {
	ID        int64  `json:"id"`
	Level     int64  `json:"level"`
	Timestamp string `json:"timestamp"`
	Hash      string `json:"hash"`
	Sender    struct {
		Address string `json:"address"`
	} `json:"sender"`
	Target *struct {
		Address string `json:"address"`
	} `json:"target"`
	Amount   int64  `json:"amount"`
	BakerFee int64  `json:"bakerFee"`
	GasUsed  int64  `json:"gasUsed"`
	Status   string `json:"status"`
}

func main() {
	dataPath := filepath.Join("test_data", "tezos_txs.jsonl")

	// Load transactions.
	fmt.Println("Loading transactions...")
	txs := loadTransactions(dataPath)
	fmt.Printf("Loaded %d transactions\n\n", len(txs))

	// Find a "hot" sender and target for realistic queries.
	senderCount := map[string]int{}
	targetCount := map[string]int{}
	for _, tx := range txs {
		senderCount[tx.Sender.Address]++
		if tx.Target != nil {
			targetCount[tx.Target.Address]++
		}
	}
	hotSender := topKey(senderCount)
	// Find a target the hot sender actually sends to.
	senderTargetCount := map[string]int{}
	for _, tx := range txs {
		if tx.Sender.Address == hotSender && tx.Target != nil {
			senderTargetCount[tx.Target.Address]++
		}
	}
	hotTarget := topKey(senderTargetCount)
	if hotTarget == "" {
		hotTarget = topKey(targetCount)
	}
	// Pick a level range that the hot sender has txs in.
	// Find levels where hotSender appears and pick a range covering ~10% of those.
	var senderLevels []int64
	for _, tx := range txs {
		if tx.Sender.Address == hotSender {
			senderLevels = append(senderLevels, tx.Level)
		}
	}
	sort.Slice(senderLevels, func(i, j int) bool { return senderLevels[i] < senderLevels[j] })
	// Pick range covering ~10% of sender's levels (guaranteed non-empty intersection).
	rangeLo := senderLevels[len(senderLevels)/4]
	rangeHi := senderLevels[len(senderLevels)/4+len(senderLevels)/10]

	fmt.Printf("Query parameters:\n")
	fmt.Printf("  Hot sender: %s (%d txs)\n", hotSender, senderCount[hotSender])
	fmt.Printf("  Hot target: %s (%d txs)\n", hotTarget, targetCount[hotTarget])
	fmt.Printf("  Level range: %d-%d\n\n", rangeLo, rangeHi)

	tmpDir, _ := os.MkdirTemp("", "bench_compare_*")
	defer os.RemoveAll(tmpDir)

	// ── FST ────────────────────────────────────────────
	fmt.Println("═══ FST Index ═══")
	fstDir := filepath.Join(tmpDir, "fst")
	benchFST(fstDir, txs, hotSender, hotTarget, rangeLo, rangeHi)

	runtime.GC()

	// ── SQLite ─────────────────────────────────────────
	fmt.Println("\n═══ SQLite ═══")
	sqlDir := filepath.Join(tmpDir, "sqlite")
	os.MkdirAll(sqlDir, 0755)
	benchSQLite(filepath.Join(sqlDir, "txs.db"), txs, hotSender, hotTarget, rangeLo, rangeHi)
}

// ── FST benchmark ──────────────────────────────────────

func benchFST(dir string, txs []Transaction, hotSender, hotTarget string, rangeLo, rangeHi int64) {
	store, err := set.Open(dir)
	if err != nil {
		panic(err)
	}

	repo, err := store.Repo(set.Schema{
		Name: "transactions",
		Fields: []set.Field{
			{Name: "sender", Type: set.String(dict.KeyRaw)},
			{Name: "target", Type: set.String(dict.KeyRaw)},
			{Name: "level", Type: set.Range(set.Uint64BE)},
			{Name: "status", Type: set.Enum("applied", "failed", "backtracked", "skipped")},
		},
		Composites: []set.Composite{
			{Fields: []string{"sender", "target"}},
		},
	})
	if err != nil {
		panic(err)
	}

	// Index using sequential IDs for dense bitmap bucketing.
	t0 := time.Now()
	indexed := 0
	skipped := 0
	for i, tx := range txs {
		target := "none"
		if tx.Target != nil {
			target = tx.Target.Address
		}
		err = repo.Index(uint64(i), set.Record{
			"sender": tx.Sender.Address,
			"target": target,
			"level":  uint64(tx.Level),
			"status": tx.Status,
		})
		if err != nil {
			skipped++
			continue
		}
		indexed++
	}
	if skipped > 0 {
		fmt.Printf("  Skipped: %d records (codec errors)\n", skipped)
	}
	indexTime := time.Since(t0)
	fmt.Printf("  Index:   %s (%.0f txs/sec)\n", indexTime, float64(len(txs))/indexTime.Seconds())

	// Flush.
	t0 = time.Now()
	repo.Flush()
	flushTime := time.Since(t0)
	fmt.Printf("  Flush:   %s\n", flushTime)
	fmt.Printf("  Total write: %s\n", indexTime+flushTime)

	// Disk size.
	size := dirSize(dir)
	fmt.Printf("  Disk:    %s\n", formatBytes(size))

	// Compact.
	t0 = time.Now()
	repo.Compact()
	fmt.Printf("  Compact: %s\n", time.Since(t0))
	size = dirSize(dir)
	fmt.Printf("  Disk after compact: %s\n", formatBytes(size))

	fmt.Printf("  Indexed: %d records\n", indexed)

	// Queries.
	fmt.Println("  Queries:")

	// Eq: sender
	benchQuery("    Eq(sender)", func() int {
		iter := repo.Query(set.Eq("sender", hotSender))
		n := 0
		for iter.Next() {
			_ = iter.ID()
			n++
		}
		iter.Close()
		return n
	})

	// And: sender AND target
	benchQuery("    And(sender,target)", func() int {
		iter := repo.Query(set.And(
			set.Eq("sender", hotSender),
			set.Eq("target", hotTarget),
		))
		n := 0
		for iter.Next() {
			_ = iter.ID()
			n++
		}
		iter.Close()
		return n
	})

	// Range: level
	benchQuery("    Range(level)", func() int {
		iter := repo.Query(set.Between("level", uint64(rangeLo), uint64(rangeHi)))
		n := 0
		for iter.Next() {
			_ = iter.ID()
			n++
		}
		iter.Close()
		return n
	})

	// Combined: sender AND level range
	benchQuery("    And(sender,level range)", func() int {
		iter := repo.Query(set.And(
			set.Eq("sender", hotSender),
			set.Between("level", uint64(rangeLo), uint64(rangeHi)),
		))
		n := 0
		for iter.Next() {
			_ = iter.ID()
			n++
		}
		iter.Close()
		return n
	})

	store.Close()
}

// ── SQLite benchmark ───────────────────────────────────

func benchSQLite(dbPath string, txs []Transaction, hotSender, hotTarget string, rangeLo, rangeHi int64) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal=WAL&_sync=OFF")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.Exec("PRAGMA page_size = 4096")
	db.Exec("PRAGMA mmap_size = 1073741824")
	db.Exec("PRAGMA cache_size = -64000")

	db.Exec(`CREATE TABLE txs (
		hash_id INTEGER PRIMARY KEY,
		hash TEXT NOT NULL,
		sender TEXT NOT NULL,
		target TEXT NOT NULL,
		level INTEGER NOT NULL,
		status TEXT NOT NULL
	)`)

	// Insert.
	t0 := time.Now()
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO txs (hash_id, hash, sender, target, level, status) VALUES (?, ?, ?, ?, ?, ?)")
	for i, t := range txs {
		target := ""
		if t.Target != nil {
			target = t.Target.Address
		}
		stmt.Exec(i, t.Hash, t.Sender.Address, target, t.Level, t.Status)
	}
	stmt.Close()
	tx.Commit()
	insertTime := time.Since(t0)
	fmt.Printf("  Insert:  %s (%.0f txs/sec)\n", insertTime, float64(len(txs))/insertTime.Seconds())

	// Create indexes.
	t0 = time.Now()
	db.Exec("CREATE INDEX idx_sender ON txs(sender)")
	db.Exec("CREATE INDEX idx_target ON txs(target)")
	db.Exec("CREATE INDEX idx_level ON txs(level)")
	db.Exec("CREATE INDEX idx_status ON txs(status)")
	db.Exec("CREATE INDEX idx_sender_target ON txs(sender, target)")
	db.Exec("CREATE INDEX idx_sender_level ON txs(sender, level)")
	indexTime := time.Since(t0)
	fmt.Printf("  Indexes: %s\n", indexTime)
	fmt.Printf("  Total write: %s\n", insertTime+indexTime)

	// Disk size.
	info, _ := os.Stat(dbPath)
	walInfo, _ := os.Stat(dbPath + "-wal")
	size := info.Size()
	if walInfo != nil {
		size += walInfo.Size()
	}
	fmt.Printf("  Disk:    %s\n", formatBytes(size))

	// Queries.
	fmt.Println("  Queries:")

	// Eq: sender
	benchQuery("    Eq(sender)", func() int {
		rows, _ := db.Query("SELECT hash_id FROM txs WHERE sender = ?", hotSender)
		n := 0
		for rows.Next() {
			var id int
			rows.Scan(&id)
			n++
		}
		rows.Close()
		return n
	})

	// And: sender AND target
	benchQuery("    And(sender,target)", func() int {
		rows, _ := db.Query("SELECT hash_id FROM txs WHERE sender = ? AND target = ?", hotSender, hotTarget)
		n := 0
		for rows.Next() {
			var id int
			rows.Scan(&id)
			n++
		}
		rows.Close()
		return n
	})

	// Range: level
	benchQuery("    Range(level)", func() int {
		rows, _ := db.Query("SELECT hash_id FROM txs WHERE level BETWEEN ? AND ?", rangeLo, rangeHi)
		n := 0
		for rows.Next() {
			var id int
			rows.Scan(&id)
			n++
		}
		rows.Close()
		return n
	})

	// Combined: sender AND level range
	benchQuery("    And(sender,level range)", func() int {
		rows, _ := db.Query("SELECT hash_id FROM txs WHERE sender = ? AND level BETWEEN ? AND ?", hotSender, rangeLo, rangeHi)
		n := 0
		for rows.Next() {
			var id int
			rows.Scan(&id)
			n++
		}
		rows.Close()
		return n
	})
}

// ── Helpers ────────────────────────────────────────────

func benchQuery(label string, fn func() int) {
	// Warmup.
	resultCount := fn()

	// Timed run: repeat enough times to get stable measurement.
	runs := 1000
	t0 := time.Now()
	for i := 0; i < runs; i++ {
		fn()
	}
	elapsed := time.Since(t0)
	avg := elapsed / time.Duration(runs)
	fmt.Printf("%s: %v avg (%d results)\n", label, avg, resultCount)
}

func loadTransactions(path string) []Transaction {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var txs []Transaction
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		var tx Transaction
		if err := json.Unmarshal(scanner.Bytes(), &tx); err != nil {
			continue
		}
		txs = append(txs, tx)
	}
	return txs
}

func topKey(m map[string]int) string {
	var best string
	var bestN int
	for k, n := range m {
		if n > bestN {
			best, bestN = k, n
		}
	}
	return best
}

func dirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
