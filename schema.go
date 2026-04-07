package set

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/tulpenhaendler/dict"
)

type Schema struct {
	Name       string
	Fields     []Field
	Composites []Composite // composite indexes over multiple fields
}

// Composite defines a composite index over multiple fields.
// Enables single-lookup And queries instead of bitmap intersection.
type Composite struct {
	Fields []string // field names, order matters (leftmost prefix queryable)
}

type Field struct {
	Name   string
	Type   FieldType
	Unique bool
}

type FieldType struct {
	Kind     FieldKind
	DictKey  dict.KeyType
	RangeEnc RangeEncoding
	EnumVals []string
	ByteSize int
	StoredAs StoredKind // only for KindStored: range vs string
}

// StoredKind distinguishes numeric vs string stored fields.
type StoredKind byte

const (
	StoredAsRange  StoredKind = iota // uses RangeEnc (Uint64BE, Int64BE, Timestamp)
	StoredAsString                   // uses DictKey codec for encode/decode
)

type FieldKind byte

const (
	KindString FieldKind = iota
	KindSlice
	KindRange
	KindBool
	KindEnum
	KindBytes
	KindBigInt
	KindStored
)

type RangeEncoding byte

const (
	Uint64BE RangeEncoding = iota
	Int64BE
	Timestamp
)

func String(k dict.KeyType) FieldType {
	return FieldType{Kind: KindString, DictKey: k}
}

func Slice(k dict.KeyType) FieldType {
	return FieldType{Kind: KindSlice, DictKey: k}
}

func Range(enc RangeEncoding) FieldType {
	return FieldType{Kind: KindRange, RangeEnc: enc}
}

func Bool() FieldType {
	return FieldType{Kind: KindBool}
}

func Enum(vals ...string) FieldType {
	return FieldType{Kind: KindEnum, EnumVals: vals}
}

func Bytes(size int) FieldType {
	return FieldType{Kind: KindBytes, ByteSize: size}
}

func BigInt() FieldType {
	return FieldType{Kind: KindBigInt}
}

// Stored creates a stored (non-indexed) field type. Values are persisted
// alongside indexed fields but do not produce FST/bitmap entries.
// enc must be a RangeEncoding (for numeric types) or dict.KeyType (for strings).
func Stored(enc any) FieldType {
	switch e := enc.(type) {
	case RangeEncoding:
		return FieldType{Kind: KindStored, RangeEnc: e, StoredAs: StoredAsRange}
	case dict.KeyType:
		return FieldType{Kind: KindStored, DictKey: e, StoredAs: StoredAsString}
	default:
		panic(fmt.Sprintf("set.Stored: unsupported encoding type %T", enc))
	}
}

// Hash returns a deterministic 16-char hex hash of the schema.
// Fields are sorted by name so declaration order doesn't matter.
func (s Schema) Hash() string {
	h := sha256.New()
	h.Write([]byte(s.Name))

	sorted := make([]Field, len(s.Fields))
	copy(sorted, s.Fields)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})

	for _, f := range sorted {
		h.Write([]byte(f.Name))
		h.Write([]byte{byte(f.Type.Kind)})
		h.Write([]byte{byte(f.Type.DictKey)})
		h.Write([]byte{byte(f.Type.RangeEnc)})
		for _, v := range f.Type.EnumVals {
			h.Write([]byte(v))
			h.Write([]byte{0}) // separator
		}
		h.Write([]byte{byte(f.Type.ByteSize >> 8), byte(f.Type.ByteSize)})
		h.Write([]byte{byte(f.Type.StoredAs)})
	}

	// Include composites in hash.
	for _, c := range s.Composites {
		h.Write([]byte("composite"))
		for _, name := range c.Fields {
			h.Write([]byte(name))
			h.Write([]byte{0})
		}
	}

	return hex.EncodeToString(h.Sum(nil))[:16]
}

// MarshalJSON returns a JSON representation of the schema for on-disk storage.
func (s Schema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name       string      `json:"name"`
		Fields     []Field     `json:"fields"`
		Composites []Composite `json:"composites,omitempty"`
		Hash       string      `json:"hash"`
	}{s.Name, s.Fields, s.Composites, s.Hash()})
}

// Record is a map of field name → value for indexing.
type Record map[string]any

// ValidateRecord checks that a record matches the schema.
func (s Schema) ValidateRecord(rec Record) error {
	for _, f := range s.Fields {
		v, ok := rec[f.Name]
		if !ok {
			return fmt.Errorf("fst: missing field %q", f.Name)
		}
		if err := validateFieldValue(f, v); err != nil {
			return fmt.Errorf("fst: field %q: %w", f.Name, err)
		}
	}
	return nil
}

func validateFieldValue(f Field, v any) error {
	switch f.Type.Kind {
	case KindString:
		if _, ok := v.(string); !ok {
			return fmt.Errorf("expected string, got %T", v)
		}
	case KindSlice:
		if _, ok := v.([]string); !ok {
			return fmt.Errorf("expected []string, got %T", v)
		}
	case KindRange:
		switch f.Type.RangeEnc {
		case Uint64BE:
			if _, ok := v.(uint64); !ok {
				return fmt.Errorf("expected uint64, got %T", v)
			}
		case Int64BE:
			if _, ok := v.(int64); !ok {
				return fmt.Errorf("expected int64, got %T", v)
			}
		case Timestamp:
			if _, ok := v.(time.Time); !ok {
				return fmt.Errorf("expected time.Time, got %T", v)
			}
		}
	case KindBool:
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("expected bool, got %T", v)
		}
	case KindEnum:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", v)
		}
		found := false
		for _, e := range f.Type.EnumVals {
			if e == s {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("invalid enum value %q", s)
		}
	case KindBytes:
		b, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("expected []byte, got %T", v)
		}
		if len(b) != f.Type.ByteSize {
			return fmt.Errorf("expected %d bytes, got %d", f.Type.ByteSize, len(b))
		}
	case KindBigInt:
		if _, ok := v.(*big.Int); !ok {
			return fmt.Errorf("expected *big.Int, got %T", v)
		}
	case KindStored:
		return validateStoredValue(f.Type, v)
	default:
		return fmt.Errorf("unknown field kind %d", f.Type.Kind)
	}
	return nil
}

func validateStoredValue(ft FieldType, v any) error {
	switch ft.StoredAs {
	case StoredAsRange:
		switch ft.RangeEnc {
		case Uint64BE:
			if _, ok := v.(uint64); !ok {
				return fmt.Errorf("expected uint64, got %T", v)
			}
		case Int64BE:
			if _, ok := v.(int64); !ok {
				return fmt.Errorf("expected int64, got %T", v)
			}
		case Timestamp:
			if _, ok := v.(time.Time); !ok {
				return fmt.Errorf("expected time.Time, got %T", v)
			}
		}
	case StoredAsString:
		if _, ok := v.(string); !ok {
			return fmt.Errorf("expected string, got %T", v)
		}
	}
	return nil
}
