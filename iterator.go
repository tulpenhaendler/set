package set

import "github.com/RoaringBitmap/roaring/v2/roaring64"

// Iterator iterates over record IDs matching a query.
type Iterator struct {
	bitmap *roaring64.Bitmap
	iter   roaring64.IntIterable64
	cur    uint64
	valid  bool

	// Stored-field context, set by Repo.Query().
	// Nil for cross-repo iterators (Intersect/Union).
	segments []*segment
	buffer   []bufferedRecord
	flushing []bufferedRecord
	schema   *Schema
	storedMap map[string]int // stored field name → index in storedNames
}

func newIterator(bm *roaring64.Bitmap) *Iterator {
	return &Iterator{
		bitmap: bm,
		iter:   bm.Iterator(),
	}
}

// Next advances the iterator. Returns false when done.
func (it *Iterator) Next() bool {
	if it.iter.HasNext() {
		it.cur = it.iter.Next()
		it.valid = true
		return true
	}
	it.valid = false
	return false
}

// ID returns the current record ID.
func (it *Iterator) ID() uint64 {
	return it.cur
}

// Stored returns the decoded value of a stored (non-indexed) field for the
// current record. Returns nil if the field is not found or the iterator
// lacks stored-field context (e.g. cross-repo iterators).
func (it *Iterator) Stored(field string) any {
	if !it.valid || it.schema == nil {
		return nil
	}
	si, ok := it.storedMap[field]
	if !ok {
		return nil
	}

	// Find the field type for decoding.
	var ft FieldType
	for _, f := range it.schema.Fields {
		if f.Name == field {
			ft = f.Type
			break
		}
	}

	// Check in-memory buffers first.
	for _, bufs := range [2][]bufferedRecord{it.buffer, it.flushing} {
		for i := range bufs {
			if bufs[i].id == it.cur && bufs[i].stored != nil {
				val, err := DecodeStoredValue(ft, bufs[i].stored[si])
				if err != nil {
					return nil
				}
				return val
			}
		}
	}

	// Search on-disk segments.
	for _, seg := range it.segments {
		sc := seg.stored[field]
		if sc == nil {
			continue
		}
		if raw, ok := sc.lookup(it.cur); ok {
			val, err := DecodeStoredValue(ft, raw)
			if err != nil {
				return nil
			}
			return val
		}
	}
	return nil
}

// Close releases resources.
func (it *Iterator) Close() error {
	return nil
}
