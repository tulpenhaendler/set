package set

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	lru "github.com/hashicorp/golang-lru/v2"

	ifst "github.com/tulpenhaendler/set/internal/fst"
)

const bitmapCacheSize = 4096

var readerPool = sync.Pool{
	New: func() any { return bytes.NewReader(nil) },
}

// segment is an immutable on-disk index segment.
type segment struct {
	dir    string
	fields map[string]*fieldIndex
	allIDs *roaring64.Bitmap // all record IDs in this segment
}

// fieldIndex holds an FST + roaring bitmaps for one field.
type fieldIndex struct {
	fst      *ifst.FST
	fstData  []byte // raw data
	roarData []byte // raw roaring file data
	count    uint32 // number of bitmaps
	cache    *lru.Cache[uint32, *roaring64.Bitmap]
}

func openSegment(dir string, fieldNames []string) (*segment, error) {
	seg := &segment{
		dir:    dir,
		fields: make(map[string]*fieldIndex),
	}

	for _, name := range fieldNames {
		fi, err := openFieldIndex(dir, name)
		if err != nil {
			seg.close()
			return nil, fmt.Errorf("fst: open field %q: %w", name, err)
		}
		if fi != nil {
			seg.fields[name] = fi
		}
	}

	// Load all-IDs bitmap if present.
	allPath := filepath.Join(dir, "_all.roar")
	if data, err := os.ReadFile(allPath); err == nil && len(data) > 0 {
		seg.allIDs = decodeBitmap(data)
	}

	return seg, nil
}

// mmapFile maps a file into memory as read-only.
func mmapFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(fi.Size())
	if size == 0 {
		return nil, nil
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %w", err)
	}
	return data, nil
}

func openFieldIndex(dir, name string) (*fieldIndex, error) {
	fstPath := filepath.Join(dir, name+".fst")
	roarPath := filepath.Join(dir, name+".roar")

	if _, err := os.Stat(fstPath); os.IsNotExist(err) {
		return nil, nil
	}

	fstData, err := mmapFile(fstPath)
	if err != nil {
		return nil, fmt.Errorf("mmap fst: %w", err)
	}

	f, err := ifst.Load(fstData)
	if err != nil {
		syscall.Munmap(fstData)
		return nil, fmt.Errorf("load fst: %w", err)
	}

	roarData, err := mmapFile(roarPath)
	if err != nil {
		syscall.Munmap(fstData)
		return nil, fmt.Errorf("mmap roar: %w", err)
	}

	var count uint32
	if len(roarData) >= 4 {
		count = binary.LittleEndian.Uint32(roarData[:4])
	}

	c, _ := lru.New[uint32, *roaring64.Bitmap](bitmapCacheSize)
	return &fieldIndex{
		fst:      f,
		fstData:  fstData,
		roarData: roarData,
		count:    count,
		cache:    c,
	}, nil
}

// lookupEq ORs all bucket bitmaps for a field value into dst.
func (fi *fieldIndex) lookupEq(valueKey []byte, dst *roaring64.Bitmap) {
	it := fi.fst.IteratorPrefix(valueKey)
	for it.Next() {
		dst.Or(fi.loadBitmap(uint32(it.Value())))
	}
}

// probeEqBuckets intersects candidates with a value using bucket-level pruning.
// Uses a pooled tmp bitmap to avoid per-iteration allocations.
func (fi *fieldIndex) probeEqBuckets(valueKey []byte, candidateBuckets *roaring64.Bitmap, candidates *roaring64.Bitmap, result *roaring64.Bitmap) {
	tmp := getBitmap()
	it := fi.fst.IteratorPrefix(valueKey)
	for it.Next() {
		compositeKey := it.Key()
		if len(compositeKey) <= len(valueKey) {
			continue
		}
		bucket := DecodeKey(compositeKey[len(valueKey):])
		if !candidateBuckets.Contains(bucket) {
			continue
		}
		bm := fi.loadBitmap(uint32(it.Value()))
		tmp.Or(bm)
		tmp.And(candidates)
		result.Or(tmp)
		tmp.Clear()
	}
	bitmapPool.Put(tmp)
}

// lookupRange ORs all bitmaps for keys in [from, to) into dst.
func (fi *fieldIndex) lookupRange(from, to []byte, dst *roaring64.Bitmap) {
	it := fi.fst.Iterator(from, to)
	for it.Next() {
		dst.Or(fi.loadBitmap(uint32(it.Value())))
	}
}

// lookupAll ORs all bitmaps into dst.
func (fi *fieldIndex) lookupAll(dst *roaring64.Bitmap) {
	it := fi.fst.Iterator(nil, nil)
	for it.Next() {
		dst.Or(fi.loadBitmap(uint32(it.Value())))
	}
}

// estimateEq returns estimated cardinality by summing bucket bitmap sizes.
func (fi *fieldIndex) estimateEq(valueKey []byte) uint64 {
	var est uint64
	it := fi.fst.IteratorPrefix(valueKey)
	for it.Next() {
		size := fi.estimateBitmapSize(uint32(it.Value()))
		if size <= 24 {
			est += 1
		} else {
			est += uint64(size-24) / 2
		}
	}
	return est
}

func (fi *fieldIndex) estimateBitmapSize(idx uint32) int64 {
	if idx >= fi.count {
		return 0
	}
	offPos := 4 + int(idx)*8
	if offPos+8 > len(fi.roarData) {
		return 0
	}
	offset := binary.LittleEndian.Uint64(fi.roarData[offPos : offPos+8])
	var end uint64
	if idx+1 < fi.count {
		end = binary.LittleEndian.Uint64(fi.roarData[offPos+8 : offPos+16])
	} else {
		end = uint64(len(fi.roarData))
	}
	return int64(end - offset)
}

func (fi *fieldIndex) loadBitmap(idx uint32) *roaring64.Bitmap {
	if idx >= fi.count {
		return roaring64.New()
	}

	if bm, ok := fi.cache.Get(idx); ok {
		return bm
	}

	offPos := 4 + int(idx)*8
	if offPos+8 > len(fi.roarData) {
		return roaring64.New()
	}
	offset := binary.LittleEndian.Uint64(fi.roarData[offPos : offPos+8])

	var end uint64
	if idx+1 < fi.count {
		nextOffPos := offPos + 8
		if nextOffPos+8 > len(fi.roarData) {
			return roaring64.New()
		}
		end = binary.LittleEndian.Uint64(fi.roarData[nextOffPos : nextOffPos+8])
	} else {
		end = uint64(len(fi.roarData))
	}

	if offset >= end || int(offset) >= len(fi.roarData) || int(end) > len(fi.roarData) {
		return roaring64.New()
	}

	bm := decodeBitmap(fi.roarData[offset:end])
	if bm == nil {
		return roaring64.New()
	}
	fi.cache.Add(idx, bm)
	return bm
}

// decodeBitmap deserializes a roaring bitmap, returning nil on corrupted data.
func decodeBitmap(data []byte) (bm *roaring64.Bitmap) {
	defer func() {
		if recover() != nil {
			bm = nil
		}
	}()
	bm = roaring64.New()
	r := readerPool.Get().(*bytes.Reader)
	r.Reset(data)
	if _, err := bm.ReadFrom(r); err != nil {
		readerPool.Put(r)
		return nil
	}
	readerPool.Put(r)
	return bm
}

func (fi *fieldIndex) close() {
	if fi.fstData != nil {
		syscall.Munmap(fi.fstData)
	}
	if fi.roarData != nil {
		syscall.Munmap(fi.roarData)
	}
}

func (s *segment) close() {
	for _, fi := range s.fields {
		fi.close()
	}
}
