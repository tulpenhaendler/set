# set

A secondary index engine for querying large sets of blockchain data. Returns matching record IDs, not row data -- pair it with your own storage for payloads.

Given a schema of typed fields, `set` builds [FST](https://en.wikipedia.org/wiki/Finite-state_transducer)-backed inverted indexes with [roaring bitmaps](https://roaringbitmap.org/) and answers set queries (equality, range, intersection, union) over millions of records in microseconds.

## Why not a database?

Databases use B-trees, which are optimized for mutable data -- random inserts, updates, deletes. That flexibility costs: queries walk leaf pages row by row, and multi-column intersections require index merges.

Blockchain data is immutable. By dropping the mutability requirement, `set` uses FST-backed inverted indexes with compressed bitmaps instead: an `And` of two predicates is a CPU-level bitwise AND over compact bitsets, not a merge join. FSTs are costly to build but compact and fast to query. The tradeoff is slower writes (~1.3x vs SQLite), which is acceptable when reads vastly outnumber writes.

### Benchmark: 500k Tezos transactions

| Query | FST | SQLite | Speedup |
|---|---|---|---|
| Eq(sender) | 1.1 ms | 18.8 ms | **17x** |
| And(sender,target) | 2.6 us | 7.1 us | **2.7x** |
| Range(level) | 136 us | 2.9 ms | **21x** |
| And(sender,range) | 727 us | 1.9 ms | **2.6x** |
| Disk | 78 MB | 265 MB | **3.4x smaller** |
| Total write | 2.1 s | 1.6 s | SQLite 1.3x faster |

This is a quick benchmark indicative of the performance difference between bitmap indexes and a naive SQLite implementation for this specific use case -- append-only data with filter-heavy read queries. It is not a comprehensive comparison against tuned database deployments.

Reproduce with `go run ./cmd/bench_compare` (requires `test_data/tezos_txs.jsonl`).

## Usage

```go
import (
    "github.com/tulpenhaendler/set"
    "github.com/tulpenhaendler/dict"
)

// Open a store (creates directory if needed).
store, _ := set.Open("/path/to/index")
defer store.Close()

// Define a schema.
repo, _ := store.Repo(set.Schema{
    Name: "transactions",
    Fields: []set.Field{
        {Name: "sender", Type: set.String(dict.KeyRaw)},
        {Name: "target", Type: set.String(dict.KeyRaw)},
        {Name: "level",  Type: set.Range(set.Uint64BE)},
        {Name: "status", Type: set.Enum("applied", "failed", "backtracked")},
    },
})

// Index records into an in-memory buffer.
repo.Index(0, set.Record{
    "sender": "tz1abc...",
    "target": "tz1def...",
    "level":  uint64(4_500_000),
    "status": "applied",
})

// Flush the buffer to an immutable on-disk segment.
repo.Flush()

// Query.
iter := repo.Query(set.And(
    set.Eq("sender", "tz1abc..."),
    set.Gte("level", uint64(4_000_000)),
))
for iter.Next() {
    fmt.Println(iter.ID())
}
iter.Close()
```

## Field types

| Constructor | Go type | Description |
|---|---|---|
| `String(keyType)` | `string` | Dictionary-encoded string |
| `Slice(keyType)` | `[]string` | Multi-value string (tags, labels) |
| `Range(enc)` | `uint64`, `int64`, `time.Time` | Sortable numeric, supports range queries |
| `Bool()` | `bool` | Boolean flag |
| `Enum(vals...)` | `string` | Fixed set of values, single-byte encoding |
| `Bytes(size)` | `[]byte` | Fixed-size byte field (hashes, addresses) |
| `BigInt()` | `*big.Int` | Arbitrary-precision integer |

Range encodings: `Uint64BE`, `Int64BE`, `Timestamp`.

## Query API

```go
set.Eq("field", value)           // equality
set.Gt("field", value)           // greater than
set.Gte("field", value)          // greater than or equal
set.Lt("field", value)           // less than
set.Lte("field", value)          // less than or equal
set.Between("field", lo, hi)     // inclusive range [lo, hi]
set.Prefix("field", []byte{...}) // byte prefix match
set.And(q1, q2, ...)             // intersection
set.Or(q1, q2, ...)              // union
set.Not(q)                       // negation
```

`And` queries automatically reorder predicates by estimated selectivity and use bucket-level pruning to skip irrelevant bitmap regions.

## Composite indexes

For frequent multi-field equality queries, define a composite index to avoid bitmap intersection entirely:

```go
store.Repo(set.Schema{
    Name: "transactions",
    Fields: []set.Field{
        {Name: "sender", Type: set.String(dict.KeyRaw)},
        {Name: "target", Type: set.String(dict.KeyRaw)},
    },
    Composites: []set.Composite{
        {Fields: []string{"sender", "target"}},
    },
})

// And(Eq(sender), Eq(target)) uses a single FST lookup instead of intersecting two bitmaps.
```

## Cross-repo queries

Multiple repos in the same store share a dictionary. Use shared record IDs (e.g. dictionary-assigned IDs for transaction hashes) to intersect across repos:

```go
txIter  := txRepo.Query(set.Eq("sender", "tz1abc..."))
tagIter := tagRepo.Query(set.Eq("tag", "defi"))
result  := store.Intersect(txIter, tagIter)
```

## Write lifecycle

```
Index() → buffer in memory (fast, ~1.8M records/sec)
Flush() → write buffer to immutable segment on disk
Compact() → merge similar-sized segments (size-tiered)
```

Flush when you've accumulated a batch (e.g. every N blocks). Compact periodically to reduce the number of segments. Queries work across all segments and the in-memory buffer transparently -- records remain visible even during an in-progress flush.

## Storage

Segments are mmap'd read-only. The OS pages in only the regions touched by queries and can evict them under memory pressure. This keeps the Go heap small regardless of total index size.

On-disk format per field:
- `.fst` -- finite state transducer mapping encoded values to bitmap indices
- `.roar` -- concatenated roaring bitmap data with an offset table

FST prefix compression with varint-encoded transitions and roaring run-length encoding typically achieve 3x smaller indexes compared to equivalent B-tree indexes.

## Limitations

- **Returns IDs only.** This is a secondary index, not a database. Pair it with your own storage for row data.
- **Record IDs must be unique.** Indexing the same ID twice is not detected and will cause incorrect results after compaction. For blockchain data, use sequential IDs or block-scoped counters.
- **Schema is fixed at repo creation.** Adding or removing fields requires re-indexing. There is no migration path.
- **Cross-repo queries require shared IDs.** The [dict](https://github.com/tulpenhaendler/dict) package provides dictionary-assigned IDs for string values (transaction hashes, addresses). If you assign your own numeric IDs, cross-repo intersection works without dict.
- **Not() is universe-scoped.** It computes the full record set per segment (via precomputed allIDs bitmaps, O(segments) not O(postings)), then subtracts. Best used inside `And(selective_predicate, Not(x))` where the intersection short-circuits early.
- **No value-ordered iteration.** Results are returned in ascending record ID order, not sorted by any field value. For ordered output (e.g. "by level"), sort the IDs after fetching payloads.
- **Benchmarks measure index lookup only.** End-to-end query latency includes the payload fetch from your storage layer, which is not measured here.
