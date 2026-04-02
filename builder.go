package set

import (
	"bufio"
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
	bufs   map[string]*fieldBuffer
	arena  []byte             // shared backing store for key copies
	allIDs *roaring64.Bitmap  // all record IDs for Not() support
}

func newSegmentBuilder(dir string, fields []Field) *segmentBuilder {
	bufs := make(map[string]*fieldBuffer, len(fields))
	for _, f := range fields {
		bufs[f.Name] = &fieldBuffer{}
	}
	return &segmentBuilder{
		dir:    dir,
		fields: fields,
		bufs:   bufs,
		allIDs: roaring64.New(),
	}
}

func (sb *segmentBuilder) add(fieldName string, key []byte, recordID uint64) {
	buf := sb.bufs[fieldName]
	start := len(sb.arena)
	sb.arena = append(sb.arena, key...)
	buf.entries = append(buf.entries, fieldEntry{
		key:      sb.arena[start:len(sb.arena):len(sb.arena)],
		recordID: recordID,
	})
	if sb.allIDs != nil {
		sb.allIDs.Add(recordID)
	}
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

	// Write all-IDs bitmap for efficient Not() queries.
	if sb.allIDs != nil && !sb.allIDs.IsEmpty() {
		sb.allIDs.RunOptimize()
		allPath := filepath.Join(sb.dir, "_all.roar")
		f, err := os.Create(allPath)
		if err != nil {
			return fmt.Errorf("fst: create all-ids: %w", err)
		}
		if _, err := sb.allIDs.WriteTo(f); err != nil {
			f.Close()
			return fmt.Errorf("fst: write all-ids: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("fst: fsync all-ids: %w", err)
		}
		f.Close()
	}

	return nil
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
	// Use a local arena for composite keys to avoid per-group allocations.
	type keyBitmap struct {
		key    []byte
		bitmap *roaring64.Bitmap
	}
	var groups []keyBitmap
	var keyArena []byte
	currentIdx := -1
	var currentValueKey []byte
	currentBucket := uint64(0)

	for i := range buf.entries {
		e := &buf.entries[i]
		bucket := e.recordID / BucketSize
		if currentIdx < 0 || !bytes.Equal(currentValueKey, e.key) || currentBucket != bucket {
			start := len(keyArena)
			keyArena = append(keyArena, e.key...)
			keyArena = EncodeKeyTo(keyArena, bucket)
			ck := keyArena[start:len(keyArena):len(keyArena)]

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
	if err := fstFile.Sync(); err != nil {
		return fmt.Errorf("fsync fst: %w", err)
	}

	// Build roaring file: [count:4][offsets:8*count][bitmap data...]
	count := uint32(len(groups))
	headerLen := 4 + int(count)*8

	var bitmapData bytes.Buffer
	offsets := make([]uint64, count)
	for i, g := range groups {
		g.bitmap.RunOptimize()
		offsets[i] = uint64(headerLen) + uint64(bitmapData.Len())
		if _, err := g.bitmap.WriteTo(&bitmapData); err != nil {
			return fmt.Errorf("write bitmap %d: %w", i, err)
		}
	}

	roarPath := filepath.Join(sb.dir, name+".roar")
	roarFile, err := os.Create(roarPath)
	if err != nil {
		return err
	}
	defer roarFile.Close()

	w := bufio.NewWriterSize(roarFile, 65536)
	binary.Write(w, binary.LittleEndian, count)
	for _, off := range offsets {
		binary.Write(w, binary.LittleEndian, off)
	}
	w.Write(bitmapData.Bytes())
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush roar: %w", err)
	}
	if err := roarFile.Sync(); err != nil {
		return fmt.Errorf("fsync roar: %w", err)
	}
	return nil
}
