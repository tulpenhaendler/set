package set

import (
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/tulpenhaendler/dict"
)

// Repo is an index repository with a fixed schema.
type Repo struct {
	dir    string
	schema Schema
	dict   *dict.Dict

	mu       sync.RWMutex
	segments []*segment
	nextSeg  int
	buffer   []bufferedRecord
}

type bufferedRecord struct {
	id     uint64
	fields map[string][]byte // field name → encoded key (one per String/Range/Bool/Enum/Bytes/BigInt)
	slices map[string][][]byte // field name → encoded keys (for Slice fields)
}

func openRepo(dir string, schema Schema, d *dict.Dict) (*Repo, error) {
	r := &Repo{
		dir:    dir,
		schema: schema,
		dict:   d,
	}

	// Load existing segments.
	segDir := filepath.Join(dir, "segments")
	entries, err := os.ReadDir(segDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("fst: read segments dir: %w", err)
	}

	fieldNames := allFieldNames(schema)

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

	for _, n := range segNums {
		segPath := filepath.Join(segDir, fmt.Sprintf("%06d", n))
		seg, err := openSegment(segPath, fieldNames)
		if err != nil {
			return nil, fmt.Errorf("fst: open segment %d: %w", n, err)
		}
		r.segments = append(r.segments, seg)
		if n >= r.nextSeg {
			r.nextSeg = n + 1
		}
	}

	return r, nil
}

// Index adds a record to the in-memory buffer.
func (r *Repo) Index(id uint64, rec Record) error {
	if err := r.schema.ValidateRecord(rec); err != nil {
		return err
	}

	br := bufferedRecord{
		id:     id,
		fields: make(map[string][]byte),
		slices: make(map[string][][]byte),
	}

	for _, f := range r.schema.Fields {
		v := rec[f.Name]
		key, keys, err := r.encodeRecordField(f, v)
		if err != nil {
			return fmt.Errorf("fst: encode field %q: %w", f.Name, err)
		}
		if keys != nil {
			br.slices[f.Name] = keys
		} else {
			br.fields[f.Name] = key
		}
	}

	// Build composite keys by concatenating component field encodings.
	for _, c := range r.schema.Composites {
		var ck []byte
		for _, name := range c.Fields {
			if key, ok := br.fields[name]; ok {
				ck = append(ck, key...)
			}
		}
		br.fields[compositeFieldName(c)] = ck
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
func (r *Repo) Flush() error {
	r.mu.Lock()
	buf := r.buffer
	r.buffer = nil
	segNum := r.nextSeg
	r.nextSeg++
	r.mu.Unlock()

	if len(buf) == 0 {
		return nil
	}

	segDir := filepath.Join(r.dir, "segments", fmt.Sprintf("%06d", segNum))
	sb := newSegmentBuilder(segDir, allFields(r.schema))

	for _, br := range buf {
		for name, key := range br.fields {
			sb.add(name, key, br.id)
		}
		for name, keys := range br.slices {
			for _, key := range keys {
				sb.add(name, key, br.id)
			}
		}
	}

	if err := sb.build(); err != nil {
		return err
	}

	fieldNames := allFieldNames(r.schema)
	seg, err := openSegment(segDir, fieldNames)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.segments = append(r.segments, seg)
	r.mu.Unlock()

	return nil
}

// Compact merges all segments into a single segment.
func (r *Repo) Compact() error {
	r.mu.Lock()
	if len(r.segments) <= 1 {
		r.mu.Unlock()
		return nil
	}
	segs := r.segments
	segNum := r.nextSeg
	r.nextSeg++
	r.mu.Unlock()

	segDir := filepath.Join(r.dir, "segments", fmt.Sprintf("%06d", segNum))
	allF := allFields(r.schema)
	sb := newSegmentBuilder(segDir, allF)

	// For each field (including composites), iterate all segments' FSTs and merge.
	for _, f := range allF {
		for _, seg := range segs {
			fi := seg.fields[f.Name]
			if fi == nil {
				continue
			}
			it := fi.fst.Iterator(nil, nil)
			for it.Next() {
				bm := fi.loadBitmap(uint32(it.Value()))
				bmIt := bm.Iterator()
				for bmIt.HasNext() {
					recordID := bmIt.Next()
					sb.add(f.Name, it.Key(), recordID)
				}
			}
		}
	}

	if err := sb.build(); err != nil {
		return fmt.Errorf("fst: compact build: %w", err)
	}

	fieldNames := allFieldNames(r.schema)
	newSeg, err := openSegment(segDir, fieldNames)
	if err != nil {
		return fmt.Errorf("fst: compact open: %w", err)
	}

	// Swap segments.
	r.mu.Lock()
	oldSegs := r.segments
	r.segments = []*segment{newSeg}
	r.mu.Unlock()

	// Clean up old segments.
	for _, seg := range oldSegs {
		seg.close()
		os.RemoveAll(seg.dir)
	}

	return nil
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

