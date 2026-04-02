package set

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/RoaringBitmap/roaring/v2/roaring64"

	ifst "github.com/tulpenhaendler/set/internal/fst"
)

// BucketSize is the max number of record IDs per bitmap chunk.
// Smaller = more granular pruning in And queries, larger = fewer FST keys.
const BucketSize = 10000

// fieldBuffer accumulates (encoded_key, record_id) pairs for one field.
type fieldBuffer struct {
	entries []fieldEntry
}

type fieldEntry struct {
	key      []byte
	recordID uint64
}

// segmentBuilder builds a segment directory from buffered field data.
type segmentBuilder struct {
	dir    string
	fields []Field
	bufs   map[string]*fieldBuffer // field name → buffer
}

func newSegmentBuilder(dir string, fields []Field) *segmentBuilder {
	bufs := make(map[string]*fieldBuffer, len(fields))
	for _, f := range fields {
		bufs[f.Name] = &fieldBuffer{}
	}
	return &segmentBuilder{dir: dir, fields: fields, bufs: bufs}
}

func (sb *segmentBuilder) add(fieldName string, key []byte, recordID uint64) {
	buf := sb.bufs[fieldName]
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	buf.entries = append(buf.entries, fieldEntry{key: keyCopy, recordID: recordID})
}

func (sb *segmentBuilder) build() error {
	if err := os.MkdirAll(sb.dir, 0755); err != nil {
		return fmt.Errorf("fst: mkdir segment: %w", err)
	}

	for _, f := range sb.fields {
		buf := sb.bufs[f.Name]
		if len(buf.entries) == 0 {
			continue
		}
		if err := sb.buildField(f.Name, buf); err != nil {
			return fmt.Errorf("fst: build field %q: %w", f.Name, err)
		}
	}
	return nil
}

// compositeKey builds a bucketed FST key: encoded_value + bucket.
func compositeKey(valueKey []byte, bucket uint64) []byte {
	bucketKey := EncodeKey(bucket)
	key := make([]byte, len(valueKey)+len(bucketKey))
	copy(key, valueKey)
	copy(key[len(valueKey):], bucketKey)
	return key
}

func (sb *segmentBuilder) buildField(name string, buf *fieldBuffer) error {
	// Sort by (value_key, bucket) where bucket = recordID / BucketSize.
	sort.Slice(buf.entries, func(i, j int) bool {
		ei, ej := &buf.entries[i], &buf.entries[j]
		cmp := bytes.Compare(ei.key, ej.key)
		if cmp != 0 {
			return cmp < 0
		}
		return ei.recordID/BucketSize < ej.recordID/BucketSize
	})

	// Group by (value_key, bucket) → roaring bitmap.
	type keyBitmap struct {
		key    []byte // composite key: value + bucket
		bitmap *roaring64.Bitmap
	}
	var groups []keyBitmap
	currentIdx := -1
	var currentValueKey []byte
	currentBucket := uint64(0)

	for i := range buf.entries {
		e := &buf.entries[i]
		bucket := e.recordID / BucketSize
		if currentIdx < 0 || !bytes.Equal(currentValueKey, e.key) || currentBucket != bucket {
			ck := compositeKey(e.key, bucket)
			groups = append(groups, keyBitmap{
				key:    ck,
				bitmap: roaring64.New(),
			})
			currentIdx = len(groups) - 1
			currentValueKey = e.key
			currentBucket = bucket
		}
		groups[currentIdx].bitmap.Add(e.recordID)
	}

	// Groups are already sorted since entries were sorted by (value, bucket).

	// Build FST: composite_key → bitmap index.
	fstPath := filepath.Join(sb.dir, name+".fst")
	fstFile, err := os.Create(fstPath)
	if err != nil {
		return err
	}
	defer fstFile.Close()

	builder := ifst.NewBuilder(fstFile)
	for i, g := range groups {
		if err := builder.Add(g.key, uint64(i)); err != nil {
			return fmt.Errorf("fst add: %w", err)
		}
	}
	if _, err := builder.Finish(); err != nil {
		return fmt.Errorf("fst finish: %w", err)
	}

	// Build roaring file: [count:4][offsets:8*count][bitmap data...]
	roarPath := filepath.Join(sb.dir, name+".roar")
	roarFile, err := os.Create(roarPath)
	if err != nil {
		return err
	}
	defer roarFile.Close()

	count := uint32(len(groups))

	// Write header: count + placeholder offsets.
	if err := binary.Write(roarFile, binary.LittleEndian, count); err != nil {
		return err
	}
	offsets := make([]int64, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Write(roarFile, binary.LittleEndian, int64(0)); err != nil {
			return err
		}
	}

	// Write bitmaps, recording actual file positions.
	for i, g := range groups {
		g.bitmap.RunOptimize()
		pos, _ := roarFile.Seek(0, 1)
		offsets[i] = pos
		if _, err := g.bitmap.WriteTo(roarFile); err != nil {
			return fmt.Errorf("write bitmap %d: %w", i, err)
		}
	}

	// Patch offsets in header.
	for i, off := range offsets {
		if _, err := roarFile.Seek(int64(4+i*8), 0); err != nil {
			return err
		}
		if err := binary.Write(roarFile, binary.LittleEndian, off); err != nil {
			return err
		}
	}

	return nil
}
