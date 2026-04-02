package set

import (
	"bytes"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

var bitmapPool = sync.Pool{
	New: func() any { return roaring64.New() },
}

func getBitmap() *roaring64.Bitmap {
	bm := bitmapPool.Get().(*roaring64.Bitmap)
	bm.Clear()
	return bm
}

// Query represents a query against a repo.
type Query interface {
	eval(r *Repo) *roaring64.Bitmap
	// estimateCount returns a cheap upper-bound estimate of result cardinality.
	// Used by And to evaluate the most selective predicate first.
	estimateCount(r *Repo) uint64
}

type eqQuery struct {
	field string
	value any
}

type gtQuery struct {
	field    string
	value    any
	inclusive bool
}

type ltQuery struct {
	field    string
	value    any
	inclusive bool
}

type betweenQuery struct {
	field string
	lo, hi any
}

type prefixQuery struct {
	field string
	value []byte
}

type andQuery struct {
	queries []Query
}

type orQuery struct {
	queries []Query
}

type notQuery struct {
	inner Query
}

// Eq creates an equality query.
func Eq(field string, value any) Query {
	return &eqQuery{field: field, value: value}
}

// Gt creates a greater-than query.
func Gt(field string, value any) Query {
	return &gtQuery{field: field, value: value, inclusive: false}
}

// Gte creates a greater-than-or-equal query.
func Gte(field string, value any) Query {
	return &gtQuery{field: field, value: value, inclusive: true}
}

// Lt creates a less-than query.
func Lt(field string, value any) Query {
	return &ltQuery{field: field, value: value, inclusive: false}
}

// Lte creates a less-than-or-equal query.
func Lte(field string, value any) Query {
	return &ltQuery{field: field, value: value, inclusive: true}
}

// Between creates a range query [lo, hi] (inclusive on both ends).
func Between(field string, lo, hi any) Query {
	return &betweenQuery{field: field, lo: lo, hi: hi}
}

// Prefix creates a prefix query for Bytes fields.
func Prefix(field string, value []byte) Query {
	return &prefixQuery{field: field, value: value}
}

// And creates an intersection query.
func And(queries ...Query) Query {
	return &andQuery{queries: queries}
}

// Or creates a union query.
func Or(queries ...Query) Query {
	return &orQuery{queries: queries}
}

// Not creates a negation query.
func Not(query Query) Query {
	return &notQuery{inner: query}
}

// Query executes a query and returns an iterator over matching record IDs.
func (r *Repo) Query(q Query) *Iterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	bm := q.eval(r)

	// Also scan the in-memory buffer.
	bufBm := r.evalBuffer(q)
	if bufBm != nil {
		bm.Or(bufBm)
	}

	return newIterator(bm)
}

// Count returns the number of matching records.
func (r *Repo) Count(q Query) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	bm := q.eval(r)
	bufBm := r.evalBuffer(q)
	if bufBm != nil {
		bm.Or(bufBm)
	}
	return bm.GetCardinality()
}

// evalField evaluates fn across all segments for a field, merging results.
// Parallelizes across goroutines when there are 3+ segments.
func evalField(segments []*segment, field string, fn func(*fieldIndex, *roaring64.Bitmap)) *roaring64.Bitmap {
	result := roaring64.New()
	if len(segments) < 3 {
		for _, seg := range segments {
			if fi := seg.fields[field]; fi != nil {
				fn(fi, result)
			}
		}
		return result
	}

	bitmaps := make([]*roaring64.Bitmap, len(segments))
	var wg sync.WaitGroup
	for i, seg := range segments {
		fi := seg.fields[field]
		if fi == nil {
			continue
		}
		wg.Add(1)
		go func(i int, fi *fieldIndex) {
			defer wg.Done()
			bm := roaring64.New()
			fn(fi, bm)
			bitmaps[i] = bm
		}(i, fi)
	}
	wg.Wait()
	for _, bm := range bitmaps {
		if bm != nil {
			result.Or(bm)
		}
	}
	return result
}

// eval implementations

func (q *eqQuery) eval(r *Repo) *roaring64.Bitmap {
	key := r.encodeQueryValue(q.field, q.value)
	if key == nil {
		return roaring64.New()
	}
	return evalField(r.segments, q.field, func(fi *fieldIndex, dst *roaring64.Bitmap) {
		fi.lookupEq(key, dst)
	})
}

func (q *eqQuery) estimateCount(r *Repo) uint64 {
	key := r.encodeQueryValue(q.field, q.value)
	if key == nil {
		return 0
	}
	var est uint64
	for _, seg := range r.segments {
		fi := seg.fields[q.field]
		if fi != nil {
			est += fi.estimateEq(key)
		}
	}
	return est
}

func (q *gtQuery) estimateCount(r *Repo) uint64 {
	key := r.encodeQueryValue(q.field, q.value)
	if key == nil {
		return 0
	}
	var from []byte
	if q.inclusive {
		from = key
	} else {
		from = NextValueKey(key)
	}
	return estimateRange(r, q.field, from, nil)
}

func (q *gtQuery) eval(r *Repo) *roaring64.Bitmap {
	key := r.encodeQueryValue(q.field, q.value)
	if key == nil {
		return roaring64.New()
	}

	var from []byte
	if q.inclusive {
		from = key
	} else {
		from = NextValueKey(key)
	}

	return evalField(r.segments, q.field, func(fi *fieldIndex, dst *roaring64.Bitmap) {
		fi.lookupRange(from, nil, dst)
	})
}

func (q *ltQuery) estimateCount(r *Repo) uint64 {
	key := r.encodeQueryValue(q.field, q.value)
	if key == nil {
		return 0
	}
	var to []byte
	if q.inclusive {
		to = NextValueKey(key)
	} else {
		to = key
	}
	return estimateRange(r, q.field, nil, to)
}

func (q *ltQuery) eval(r *Repo) *roaring64.Bitmap {
	key := r.encodeQueryValue(q.field, q.value)
	if key == nil {
		return roaring64.New()
	}

	var to []byte
	if q.inclusive {
		to = NextValueKey(key)
	} else {
		to = key
	}

	return evalField(r.segments, q.field, func(fi *fieldIndex, dst *roaring64.Bitmap) {
		fi.lookupRange(nil, to, dst)
	})
}

func (q *betweenQuery) estimateCount(r *Repo) uint64 {
	loKey := r.encodeQueryValue(q.field, q.lo)
	hiKey := r.encodeQueryValue(q.field, q.hi)
	if loKey == nil || hiKey == nil {
		return 0
	}
	return estimateRange(r, q.field, loKey, NextValueKey(hiKey))
}

func (q *betweenQuery) eval(r *Repo) *roaring64.Bitmap {
	loKey := r.encodeQueryValue(q.field, q.lo)
	hiKey := r.encodeQueryValue(q.field, q.hi)
	if loKey == nil || hiKey == nil {
		return roaring64.New()
	}

	to := NextValueKey(hiKey)

	return evalField(r.segments, q.field, func(fi *fieldIndex, dst *roaring64.Bitmap) {
		fi.lookupRange(loKey, to, dst)
	})
}

func (q *prefixQuery) estimateCount(r *Repo) uint64 {
	return 1 << 32 // can't cheaply estimate prefix
}

// estimateRange sums bitmap size estimates for all FST keys in [from, to).
func estimateRange(r *Repo, field string, from, to []byte) uint64 {
	var est uint64
	for _, seg := range r.segments {
		fi := seg.fields[field]
		if fi == nil {
			continue
		}
		it := fi.fst.Iterator(from, to)
		for it.Next() {
			size := fi.estimateBitmapSize(uint32(it.Value()))
			if size <= 24 {
				est++
			} else {
				est += uint64(size-24) / 2
			}
		}
	}
	return est
}

func (q *prefixQuery) eval(r *Repo) *roaring64.Bitmap {
	return evalField(r.segments, q.field, func(fi *fieldIndex, dst *roaring64.Bitmap) {
		it := fi.fst.IteratorPrefix(q.value)
		for it.Next() {
			dst.Or(fi.loadBitmap(uint32(it.Value())))
		}
	})
}

func (q *andQuery) eval(r *Repo) *roaring64.Bitmap {
	if len(q.queries) == 0 {
		return roaring64.New()
	}
	if len(q.queries) == 1 {
		return q.queries[0].eval(r)
	}

	// Check if a composite index covers this And query.
	if bm := q.tryComposite(r); bm != nil {
		return bm
	}

	// Estimate cardinality for each sub-query and evaluate most selective first.
	type ranked struct {
		q   Query
		est uint64
	}
	subs := make([]ranked, len(q.queries))
	for i, sub := range q.queries {
		subs[i] = ranked{sub, sub.estimateCount(r)}
	}
	sort.Slice(subs, func(i, j int) bool {
		return subs[i].est < subs[j].est
	})

	// Evaluate the most selective predicate first.
	result := subs[0].q.eval(r)

	for _, s := range subs[1:] {
		if result.IsEmpty() {
			return result
		}
		if result.GetCardinality() < 10000 {
			if eq, ok := s.q.(*eqQuery); ok {
				result = probeEqBucketed(r, result, eq)
				continue
			}
		}
		result = roaring64.And(result, s.q.eval(r))
	}
	return result
}

// tryComposite checks if a composite index covers all Eq sub-queries.
func (q *andQuery) tryComposite(r *Repo) *roaring64.Bitmap {
	// Collect Eq sub-queries.
	eqs := make(map[string]*eqQuery)
	for _, sub := range q.queries {
		eq, ok := sub.(*eqQuery)
		if !ok {
			return nil // non-Eq query, can't use composite alone
		}
		eqs[eq.field] = eq
	}

	// Try each composite — check if it matches the Eq field set.
	for _, c := range r.schema.Composites {
		if len(c.Fields) != len(eqs) {
			continue
		}
		match := true
		for _, name := range c.Fields {
			if _, ok := eqs[name]; !ok {
				match = false
				break
			}
		}
		if !match {
			continue
		}

		// Build composite lookup key by concatenating encoded values in composite order.
		var ck []byte
		for _, name := range c.Fields {
			key := r.encodeQueryValue(name, eqs[name].value)
			if key == nil {
				return nil
			}
			ck = append(ck, key...)
		}

		cname := compositeFieldName(c)
		return evalField(r.segments, cname, func(fi *fieldIndex, dst *roaring64.Bitmap) {
			fi.lookupEq(ck, dst)
		})
	}

	return nil
}

// probeEqBucketed intersects candidates with an Eq predicate using bucket-level pruning.
func probeEqBucketed(r *Repo, candidates *roaring64.Bitmap, eq *eqQuery) *roaring64.Bitmap {
	key := r.encodeQueryValue(eq.field, eq.value)
	if key == nil {
		return roaring64.New()
	}

	// Build candidate bucket set using roaring (avoids map allocation).
	candidateBuckets := getBitmap()
	it := candidates.Iterator()
	for it.HasNext() {
		candidateBuckets.Add(it.Next() / BucketSize)
	}

	result := roaring64.New()
	for _, seg := range r.segments {
		fi := seg.fields[eq.field]
		if fi == nil {
			continue
		}
		fi.probeEqBuckets(key, candidateBuckets, candidates, result)
	}

	bitmapPool.Put(candidateBuckets)
	return result
}

func (q *andQuery) estimateCount(r *Repo) uint64 {
	if len(q.queries) == 0 {
		return 0
	}
	min := q.queries[0].estimateCount(r)
	for _, sub := range q.queries[1:] {
		if est := sub.estimateCount(r); est < min {
			min = est
		}
	}
	return min
}

func (q *orQuery) estimateCount(r *Repo) uint64 {
	var total uint64
	for _, sub := range q.queries {
		total += sub.estimateCount(r)
	}
	return total
}

func (q *orQuery) eval(r *Repo) *roaring64.Bitmap {
	result := roaring64.New()
	for _, sub := range q.queries {
		result.Or(sub.eval(r))
	}
	return result
}

func (q *notQuery) estimateCount(r *Repo) uint64 {
	return 1<<32 // can't cheaply estimate NOT
}

func (q *notQuery) eval(r *Repo) *roaring64.Bitmap {
	inner := q.inner.eval(r)
	all := roaring64.New()
	for _, seg := range r.segments {
		if seg.allIDs != nil {
			all.Or(seg.allIDs)
		} else {
			// Fallback for segments built before allIDs was added.
			for _, fi := range seg.fields {
				fi.lookupAll(all)
			}
		}
	}
	all.AndNot(inner)
	return all
}

// encodeQueryValue encodes a query value to an FST key.
func (r *Repo) encodeQueryValue(fieldName string, value any) []byte {
	idx, ok := r.fieldMap[fieldName]
	if !ok || idx >= r.nFields {
		return nil
	}
	f := &r.schema.Fields[idx]

	switch f.Type.Kind {
	case KindString, KindSlice:
		s, ok := value.(string)
		if !ok {
			return nil
		}
		id, err := r.dict.Get(s, f.Type.DictKey)
		if err != nil {
			return nil
		}
		return EncodeKey(id)

	case KindRange:
		switch f.Type.RangeEnc {
		case Uint64BE:
			if v, ok := value.(uint64); ok {
				return EncodeKey(v)
			}
		case Int64BE:
			if v, ok := value.(int64); ok {
				return EncodeInt64Key(v)
			}
		case Timestamp:
			if v, ok := value.(time.Time); ok {
				return EncodeKey(uint64(v.UnixNano()))
			}
		}

	case KindBool:
		if v, ok := value.(bool); ok {
			if v {
				return []byte{0x01}
			}
			return []byte{0x00}
		}

	case KindEnum:
		if s, ok := value.(string); ok {
			key, ok := EncodeEnumValue(f.Type.EnumVals, s)
			if ok {
				return key
			}
		}

	case KindBytes:
		if b, ok := value.([]byte); ok {
			return b
		}

	case KindBigInt:
		if v, ok := value.(*big.Int); ok {
			return EncodeBigInt(v)
		}
	}
	return nil
}

// evalBuffer scans the in-memory buffer and flushing buffer for matches.
func (r *Repo) evalBuffer(q Query) *roaring64.Bitmap {
	if len(r.buffer) == 0 && len(r.flushing) == 0 {
		return nil
	}
	return evalBufferQuery(r, q)
}

func evalBufferQuery(r *Repo, q Query) *roaring64.Bitmap {
	switch q := q.(type) {
	case *eqQuery:
		key := r.encodeQueryValue(q.field, q.value)
		if key == nil {
			return roaring64.New()
		}
		return scanBuffer(r, q.field, func(k []byte) bool {
			return bytes.Equal(k, key)
		})

	case *gtQuery:
		key := r.encodeQueryValue(q.field, q.value)
		if key == nil {
			return roaring64.New()
		}
		return scanBuffer(r, q.field, func(k []byte) bool {
			cmp := bytes.Compare(k, key)
			if q.inclusive {
				return cmp >= 0
			}
			return cmp > 0
		})

	case *ltQuery:
		key := r.encodeQueryValue(q.field, q.value)
		if key == nil {
			return roaring64.New()
		}
		return scanBuffer(r, q.field, func(k []byte) bool {
			cmp := bytes.Compare(k, key)
			if q.inclusive {
				return cmp <= 0
			}
			return cmp < 0
		})

	case *betweenQuery:
		loKey := r.encodeQueryValue(q.field, q.lo)
		hiKey := r.encodeQueryValue(q.field, q.hi)
		if loKey == nil || hiKey == nil {
			return roaring64.New()
		}
		return scanBuffer(r, q.field, func(k []byte) bool {
			return bytes.Compare(k, loKey) >= 0 && bytes.Compare(k, hiKey) <= 0
		})

	case *prefixQuery:
		return scanBuffer(r, q.field, func(k []byte) bool {
			return len(k) >= len(q.value) && bytes.Equal(k[:len(q.value)], q.value)
		})

	case *andQuery:
		if len(q.queries) == 0 {
			return roaring64.New()
		}
		result := evalBufferQuery(r, q.queries[0])
		for _, sub := range q.queries[1:] {
			result.And(evalBufferQuery(r, sub))
		}
		return result

	case *orQuery:
		result := roaring64.New()
		for _, sub := range q.queries {
			result.Or(evalBufferQuery(r, sub))
		}
		return result

	case *notQuery:
		inner := evalBufferQuery(r, q.inner)
		all := roaring64.New()
		for _, br := range r.buffer {
			all.Add(br.id)
		}
		for _, br := range r.flushing {
			all.Add(br.id)
		}
		all.AndNot(inner)
		return all
	}
	return roaring64.New()
}

func scanBuffer(r *Repo, fieldName string, match func([]byte) bool) *roaring64.Bitmap {
	idx, ok := r.fieldMap[fieldName]
	if !ok {
		return roaring64.New()
	}
	bm := roaring64.New()
	isSlice := idx < r.nFields && r.schema.Fields[idx].Type.Kind == KindSlice
	// Scan both the active buffer and any buffer being flushed.
	for _, buf := range [2][]bufferedRecord{r.buffer, r.flushing} {
		for _, br := range buf {
			if isSlice {
				for _, key := range br.slices[idx] {
					if match(key) {
						bm.Add(br.id)
						break
					}
				}
			} else if key := br.keys[idx]; key != nil {
				if match(key) {
					bm.Add(br.id)
				}
			}
		}
	}
	return bm
}

