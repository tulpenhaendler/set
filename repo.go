package set

import (
	"bytes"
	"fmt"
	"math/big"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/tulpenhaendler/dict"
)

const (
	compactMinMerge = 4 // merge when a tier has this many segments
	compactTierBits = 2 // bits per tier (base-4: tiers 0-3, 4-15, 16-63, ...)
)

// Repo is an index repository with a fixed schema.
type Repo struct {
	dir    string
	schema Schema
	dict   *dict.Dict

	mu        sync.RWMutex
	segments  []*segment
	nextSeg   int
	buffer    []bufferedRecord
	flushing  []bufferedRecord // buffer being flushed, visible to queries until segment is live
	flushMu   sync.Mutex       // serializes concurrent Flush calls
	compactMu sync.Mutex

	// Pre-computed field layout.
	fieldMap    map[string]int // field name → index (includes composites)
	storedMap   map[string]int // stored field name → index in storedNames
	storedNames []string       // ordered stored field names
	nFields     int            // len(schema.Fields)
	nSlots      int            // nFields + len(schema.Composites)
}

type bufferedRecord struct {
	id     uint64
	keys   [][]byte    // indexed by field position; nil for slice/stored fields
	slices [][][]byte  // indexed by field position; nil for scalar fields
	stored [][]byte    // encoded stored values, indexed by stored field position
}

func openRepo(dir string, schema Schema, d *dict.Dict) (*Repo, error) {
	nf := len(schema.Fields)
	nc := len(schema.Composites)

	fm := make(map[string]int, nf+nc)
	for i, f := range schema.Fields {
		fm[f.Name] = i
	}
	for j, c := range schema.Composites {
		fm[compositeFieldName(c)] = nf + j
	}

	sm := make(map[string]int)
	var sn []string
	for _, f := range schema.Fields {
		if f.Type.Kind == KindStored {
			sm[f.Name] = len(sn)
			sn = append(sn, f.Name)
		}
	}

	r := &Repo{
		dir:         dir,
		schema:      schema,
		dict:        d,
		fieldMap:    fm,
		storedMap:   sm,
		storedNames: sn,
		nFields:     nf,
		nSlots:      nf + nc,
	}

	// Load existing segments.
	segDir := filepath.Join(dir, "segments")

	// Recover from interrupted compaction: remove stale segments.
	if marker, err := os.ReadFile(filepath.Join(segDir, ".compact_done")); err == nil {
		for _, line := range bytes.Split(marker, []byte("\n")) {
			name := string(bytes.TrimSpace(line))
			if name != "" {
				os.RemoveAll(filepath.Join(segDir, name))
			}
		}
		os.Remove(filepath.Join(segDir, ".compact_done"))
	}

	entries, err := os.ReadDir(segDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("fst: read segments dir: %w", err)
	}

	fieldNames := indexedFieldNames(schema)

	var segNums []int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		n, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		segNums = append(segNums, n)
	}
	sort.Ints(segNums)

	loadedSegs := make(map[int]bool)
	for _, n := range segNums {
		segPath := filepath.Join(segDir, fmt.Sprintf("%06d", n))
		seg, err := openSegment(segPath, fieldNames, sn)
		if err != nil {
			// Skip corrupted segments rather than failing to open.
			continue
		}
		r.segments = append(r.segments, seg)
		loadedSegs[n] = true
		if n >= r.nextSeg {
			r.nextSeg = n + 1
		}
	}

	// Remove orphan segment directories (from crashed flushes/compactions).
	for _, n := range segNums {
		if !loadedSegs[n] {
			os.RemoveAll(filepath.Join(segDir, fmt.Sprintf("%06d", n)))
		}
	}

	return r, nil
}

// Index adds a record to the in-memory buffer.
// Each ID must be unique across all Index calls for this repo.
// Duplicate IDs are not detected and will cause incorrect query results.
func (r *Repo) Index(id uint64, rec Record) error {
	if err := r.schema.ValidateRecord(rec); err != nil {
		return err
	}

	br := bufferedRecord{
		id:     id,
		keys:   make([][]byte, r.nSlots),
		slices: make([][][]byte, r.nFields),
	}
	if len(r.storedNames) > 0 {
		br.stored = make([][]byte, len(r.storedNames))
	}

	for i, f := range r.schema.Fields {
		v := rec[f.Name]
		if f.Type.Kind == KindStored {
			enc, err := EncodeStoredValue(f.Type, v)
			if err != nil {
				return fmt.Errorf("fst: encode stored field %q: %w", f.Name, err)
			}
			br.stored[r.storedMap[f.Name]] = enc
			continue
		}
		key, keys, err := r.encodeRecordField(f, v)
		if err != nil {
			return fmt.Errorf("fst: encode field %q: %w", f.Name, err)
		}
		if keys != nil {
			br.slices[i] = keys
		} else {
			br.keys[i] = key
		}
	}

	// Build composite keys.
	for j, c := range r.schema.Composites {
		var ck []byte
		for _, name := range c.Fields {
			idx := r.fieldMap[name]
			if key := br.keys[idx]; key != nil {
				ck = append(ck, key...)
			}
		}
		br.keys[r.nFields+j] = ck
	}

	r.mu.Lock()
	r.buffer = append(r.buffer, br)
	r.mu.Unlock()
	return nil
}

// BatchIndex adds multiple records to the buffer.
func (r *Repo) BatchIndex(ids []uint64, recs []Record) error {
	if len(ids) != len(recs) {
		return fmt.Errorf("fst: ids and records length mismatch")
	}
	for i := range ids {
		if err := r.Index(ids[i], recs[i]); err != nil {
			return fmt.Errorf("fst: record %d: %w", i, err)
		}
	}
	return nil
}

// Flush writes the in-memory buffer to a new immutable segment.
// Records remain visible to queries throughout the flush.
func (r *Repo) Flush() error {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()

	r.mu.Lock()
	buf := r.buffer
	r.buffer = nil
	r.flushing = buf
	segNum := r.nextSeg
	r.nextSeg++
	r.mu.Unlock()

	if len(buf) == 0 {
		return nil
	}

	segDir := filepath.Join(r.dir, "segments", fmt.Sprintf("%06d", segNum))
	sb := newSegmentBuilder(segDir, allFields(r.schema))

	for _, br := range buf {
		for i, f := range r.schema.Fields {
			if f.Type.Kind == KindStored {
				si := r.storedMap[f.Name]
				if br.stored[si] != nil {
					sb.addStored(f.Name, br.stored[si], br.id)
				}
				continue
			}
			if br.slices[i] != nil {
				for _, key := range br.slices[i] {
					sb.add(f.Name, key, br.id)
				}
			} else if br.keys[i] != nil {
				sb.add(f.Name, br.keys[i], br.id)
			}
		}
		for j, c := range r.schema.Composites {
			if key := br.keys[r.nFields+j]; key != nil {
				sb.add(compositeFieldName(c), key, br.id)
			}
		}
	}

	if err := sb.build(); err != nil {
		os.RemoveAll(segDir)
		r.mu.Lock()
		r.buffer = append(buf, r.buffer...)
		r.flushing = nil
		r.mu.Unlock()
		return err
	}
	// Ensure segment directory and its parent entry are durable.
	syncDir(segDir)
	syncDir(filepath.Dir(segDir))

	seg, err := openSegment(segDir, indexedFieldNames(r.schema), r.storedNames)
	if err != nil {
		os.RemoveAll(segDir)
		r.mu.Lock()
		r.buffer = append(buf, r.buffer...)
		r.flushing = nil
		r.mu.Unlock()
		return err
	}

	// Clear flushing BEFORE appending segment to avoid a window where
	// a concurrent query sees both the flushing buffer and the new segment.
	r.mu.Lock()
	r.flushing = nil
	r.segments = append(r.segments, seg)
	r.mu.Unlock()

	return nil
}

// segmentTier returns the size tier for a segment with n records.
// Base-4 tiers: 0=1-3, 1=4-15, 2=16-63, etc.
func segmentTier(n uint64) int {
	if n <= 1 {
		return 0
	}
	return bits.Len64(n-1) / compactTierBits
}

// Compact runs size-tiered compaction: merges segments of similar size
// until no tier has more than compactMinMerge segments.
func (r *Repo) Compact() error {
	r.compactMu.Lock()
	defer r.compactMu.Unlock()

	for {
		merged, err := r.compactOnce()
		if err != nil {
			return err
		}
		if !merged {
			return nil
		}
	}
}

// compactOnce finds the lowest tier with >= compactMinMerge segments and merges them.
func (r *Repo) compactOnce() (bool, error) {
	r.mu.RLock()
	segs := r.segments
	r.mu.RUnlock()

	if len(segs) < compactMinMerge {
		return false, nil
	}

	// Group segments by tier.
	tiers := map[int][]*segment{}
	for _, seg := range segs {
		t := segmentTier(seg.size())
		tiers[t] = append(tiers[t], seg)
	}

	// Find lowest tier with enough segments to merge.
	bestTier := -1
	for t, group := range tiers {
		if len(group) >= compactMinMerge && (bestTier < 0 || t < bestTier) {
			bestTier = t
		}
	}
	if bestTier < 0 {
		return false, nil
	}

	return true, r.mergeSegments(tiers[bestTier])
}

// mergeSegments merges the given segments into a single new segment.
// Concurrency: mu is held briefly to grab nextSeg and again to swap segments.
// Between those sections, Flush() may append new segments -- these are not in
// mergeSet and are preserved in kept. compactMu serializes Compact() calls.
func (r *Repo) mergeSegments(toMerge []*segment) error {
	r.mu.Lock()
	segNum := r.nextSeg
	r.nextSeg++
	r.mu.Unlock()

	segDir := filepath.Join(r.dir, "segments", fmt.Sprintf("%06d", segNum))
	allF := allFields(r.schema)
	sb := newSegmentBuilder(segDir, allF)

	for _, f := range allF {
		if f.Type.Kind == KindStored {
			continue
		}
		for _, seg := range toMerge {
			fi := seg.fields[f.Name]
			if fi == nil {
				continue
			}
			it := fi.fst.Iterator(nil, nil)
			for it.Next() {
				bm := fi.loadBitmap(uint32(it.Value()))
				bmIt := bm.Iterator()
				for bmIt.HasNext() {
					sb.add(f.Name, it.Key(), bmIt.Next())
				}
			}
		}
	}

	// Merge stored fields.
	for _, name := range r.storedNames {
		for _, seg := range toMerge {
			sc := seg.stored[name]
			if sc == nil {
				continue
			}
			for i := uint32(0); i < sc.count; i++ {
				id, val := sc.entry(i)
				sb.addStored(name, val, id)
			}
		}
	}

	if err := sb.build(); err != nil {
		os.RemoveAll(segDir)
		return fmt.Errorf("fst: compact build: %w", err)
	}

	newSeg, err := openSegment(segDir, indexedFieldNames(r.schema), r.storedNames)
	if err != nil {
		os.RemoveAll(segDir)
		return fmt.Errorf("fst: compact open: %w", err)
	}

	// Write marker listing segments to remove.
	segBase := filepath.Join(r.dir, "segments")
	var marker []byte
	for _, seg := range toMerge {
		marker = append(marker, []byte(filepath.Base(seg.dir)+"\n")...)
	}
	markerPath := filepath.Join(segBase, ".compact_done")
	if err := os.WriteFile(markerPath, marker, 0644); err != nil {
		return fmt.Errorf("fst: write compact marker: %w", err)
	}
	syncDir(segBase)

	// Swap: remove merged segments, add new one.
	r.mu.Lock()
	mergeSet := make(map[*segment]bool, len(toMerge))
	for _, s := range toMerge {
		mergeSet[s] = true
	}
	kept := make([]*segment, 0, len(r.segments)-len(toMerge)+1)
	for _, s := range r.segments {
		if !mergeSet[s] {
			kept = append(kept, s)
		}
	}
	kept = append(kept, newSeg)
	r.segments = kept
	r.mu.Unlock()

	for _, seg := range toMerge {
		seg.close()
		os.RemoveAll(seg.dir)
	}
	os.Remove(markerPath)

	return nil
}

// syncDir fsyncs a directory to persist metadata changes.
func syncDir(path string) {
	if d, err := os.Open(path); err == nil {
		d.Sync()
		d.Close()
	}
}

func (r *Repo) encodeRecordField(f Field, v any) (key []byte, keys [][]byte, err error) {
	switch f.Type.Kind {
	case KindString:
		s := v.(string)
		id, err := r.dict.Get(s, f.Type.DictKey)
		if err != nil {
			return nil, nil, err
		}
		return EncodeKey(id), nil, nil

	case KindSlice:
		ss := v.([]string)
		keys := make([][]byte, len(ss))
		for i, s := range ss {
			id, err := r.dict.Get(s, f.Type.DictKey)
			if err != nil {
				return nil, nil, err
			}
			keys[i] = EncodeKey(id)
		}
		return nil, keys, nil

	case KindRange:
		switch f.Type.RangeEnc {
		case Uint64BE:
			return EncodeKey(v.(uint64)), nil, nil
		case Int64BE:
			return EncodeInt64Key(v.(int64)), nil, nil
		case Timestamp:
			return EncodeKey(uint64(v.(time.Time).UnixNano())), nil, nil
		}

	case KindBool:
		if v.(bool) {
			return []byte{0x01}, nil, nil
		}
		return []byte{0x00}, nil, nil

	case KindEnum:
		s := v.(string)
		key, ok := EncodeEnumValue(f.Type.EnumVals, s)
		if !ok {
			return nil, nil, fmt.Errorf("invalid enum value %q", s)
		}
		return key, nil, nil

	case KindBytes:
		return v.([]byte), nil, nil

	case KindBigInt:
		return EncodeBigInt(v.(*big.Int)), nil, nil
	}

	return nil, nil, fmt.Errorf("unknown field kind %d", f.Type.Kind)
}

func (r *Repo) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, seg := range r.segments {
		seg.close()
	}
}

// compositeFieldName returns the internal field name for a composite index.
func compositeFieldName(c Composite) string {
	name := "_c"
	for _, f := range c.Fields {
		name += "_" + f
	}
	return name
}

// allFields returns schema fields plus virtual fields for composites.
func allFields(s Schema) []Field {
	fields := make([]Field, len(s.Fields))
	copy(fields, s.Fields)
	for _, c := range s.Composites {
		fields = append(fields, Field{Name: compositeFieldName(c)})
	}
	return fields
}

// allFieldNames returns all field names including composite virtual fields.
func allFieldNames(s Schema) []string {
	names := make([]string, 0, len(s.Fields)+len(s.Composites))
	for _, f := range s.Fields {
		names = append(names, f.Name)
	}
	for _, c := range s.Composites {
		names = append(names, compositeFieldName(c))
	}
	return names
}

// indexedFieldNames returns non-stored field names plus composite virtual fields.
func indexedFieldNames(s Schema) []string {
	var names []string
	for _, f := range s.Fields {
		if f.Type.Kind != KindStored {
			names = append(names, f.Name)
		}
	}
	for _, c := range s.Composites {
		names = append(names, compositeFieldName(c))
	}
	return names
}
