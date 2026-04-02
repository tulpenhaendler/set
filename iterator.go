package set

import "github.com/RoaringBitmap/roaring/v2/roaring64"

// Iterator iterates over record IDs matching a query.
type Iterator struct {
	bitmap *roaring64.Bitmap
	iter   roaring64.IntIterable64
	cur    uint64
	valid  bool
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

// Close releases resources.
func (it *Iterator) Close() error {
	return nil
}
