# set

A bitmap index engine for querying large sets of blockchain data. Built for workloads where you index once and query many times -- transactions, operations, events, token transfers.

Given a schema of typed fields, `set` builds [FST](https://en.wikipedia.org/wiki/Finite-state_transducer)-backed inverted indexes with [roaring bitmaps](https://roaringbitmap.org/) and answers set queries (equality, range, intersection, union) over millions of records in microseconds.

## Why not a database?

General-purpose databases are designed for mutable, row-oriented workloads. Blockchain data is different: it's append-only, immutable, and queried far more than it's written. `set` is built for exactly this access pattern.

**SQLite / Postgres** store rows in B-trees. When a query matches thousands of records, the engine walks leaf pages and deserializes each row. Index merges for multi-column queries add further overhead. `set` stores compressed bitmap sets -- an `And` of two predicates is a CPU-level bitwise AND over compact bitsets, not a merge join. The result is 3-20x faster queries at 2.5x less disk.

**Cassandra / ScyllaDB** are built for distributed writes and partition-key lookups. They're fast at "get all rows for partition X" but poor at ad-hoc intersection queries ("sender=X AND level BETWEEN Y AND Z") because that requires cross-partition scatter-gather. `set` answers these queries locally with bitmap ops in microseconds.

**Elasticsearch** supports bitmap-like operations internally but carries enormous operational overhead -- JVM tuning, cluster management, shard rebalancing. For a single-node indexer processing blockchain data, it's the wrong tool.

The tradeoff is write speed. Building an FST is more expensive than inserting into a B-tree. For blockchain data this is the right tradeoff: blocks are immutable, data is append-only, and reads vastly outnumber writes. You flush once per batch of blocks and query constantly.

### Benchmark: 500k Tezos transactions

```
                         FST         SQLite      Speedup
Eq(sender)               1.1 ms      18.8 ms     17x
And(sender,target)        2.2 µs       6.5 µs      3x
Range(level)            136 µs        2.9 ms      21x
And(sender,range)       641 µs        1.9 ms       3x
Disk                    108 MB       265 MB       2.5x smaller
Total write             6.5 s         1.6 s       SQLite 4x faster
```

The advantage grows with data size. Bitmap operations scale linearly with set bits, while B-tree traversal adds per-row overhead. At 10M+ records, expect the query gap to widen further.

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
Compact() → merge all segments into one
```

Flush when you've accumulated a batch (e.g. every N blocks). Compact periodically to reduce the number of segments and reclaim space. Queries work across all segments and the in-memory buffer transparently.

## Storage

Segments are mmap'd read-only. The OS pages in only the regions touched by queries and can evict them under memory pressure. This keeps the Go heap small regardless of total index size.

On-disk format per field:
- `.fst` -- finite state transducer mapping encoded values to bitmap indices
- `.roar` -- concatenated roaring bitmap data with an offset table

FST prefix compression and roaring run-length encoding typically achieve 2-3x smaller indexes compared to equivalent B-tree indexes.
