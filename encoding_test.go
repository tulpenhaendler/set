package set

import (
	"bytes"
	"math"
	"math/big"
	"testing"
)

func TestEncodeKeyRoundTrip(t *testing.T) {
	tests := []uint64{
		0, 1, 42, 127, 128, 255, 256,
		65535, 65536, 70000,
		19_000_000,
		math.MaxUint32,
		math.MaxUint64,
	}
	for _, v := range tests {
		enc := EncodeKey(v)
		dec := DecodeKey(enc)
		if dec != v {
			t.Errorf("EncodeKey/DecodeKey(%d): got %d", v, dec)
		}
	}
}

func TestEncodeKeySortOrder(t *testing.T) {
	values := []uint64{0, 1, 42, 255, 256, 70000, 19_000_000, math.MaxUint32, math.MaxUint64}
	var prev []byte
	for _, v := range values {
		enc := EncodeKey(v)
		if prev != nil && bytes.Compare(prev, enc) >= 0 {
			t.Errorf("sort order violated: EncodeKey(%d) >= EncodeKey(%d)", DecodeKey(prev), v)
		}
		prev = enc
	}
}

func TestEncodeKeySize(t *testing.T) {
	tests := []struct {
		v    uint64
		want int
	}{
		{0, 2},          // [01 00]
		{42, 2},         // [01 2a]
		{255, 2},        // [01 ff]
		{256, 3},        // [02 01 00]
		{70000, 4},      // [03 01 11 70]
		{19_000_000, 5}, // [04 01 21 ea c0]
	}
	for _, tt := range tests {
		enc := EncodeKey(tt.v)
		if len(enc) != tt.want {
			t.Errorf("EncodeKey(%d): len=%d, want %d (bytes: %x)", tt.v, len(enc), tt.want, enc)
		}
	}
}

func TestInt64KeySortOrder(t *testing.T) {
	values := []int64{math.MinInt64, -1000, -1, 0, 1, 1000, math.MaxInt64}
	var prev []byte
	for _, v := range values {
		enc := EncodeInt64Key(v)
		if prev != nil && bytes.Compare(prev, enc) >= 0 {
			t.Errorf("int64 sort order violated at %d", v)
		}
		prev = enc
	}
}

func TestInt64KeyRoundTrip(t *testing.T) {
	values := []int64{math.MinInt64, -1000, -1, 0, 1, 1000, math.MaxInt64}
	for _, v := range values {
		enc := EncodeInt64Key(v)
		dec := DecodeInt64Key(enc)
		if dec != v {
			t.Errorf("Int64Key roundtrip(%d): got %d", v, dec)
		}
	}
}

func TestBigIntRoundTrip(t *testing.T) {
	values := []*big.Int{
		big.NewInt(-1_000_000),
		big.NewInt(-1),
		big.NewInt(0),
		big.NewInt(1),
		big.NewInt(1_000_000),
		new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil), // 1e18
	}
	// Add a negative 1e18
	neg1e18 := new(big.Int).Neg(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	values = append([]*big.Int{neg1e18}, values...)

	for _, v := range values {
		enc := EncodeBigInt(v)
		dec := DecodeBigInt(enc)
		if dec.Cmp(v) != 0 {
			t.Errorf("BigInt roundtrip(%s): got %s", v, dec)
		}
	}
}

func TestBigIntSortOrder(t *testing.T) {
	values := []*big.Int{
		new(big.Int).Neg(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		big.NewInt(-1_000_000),
		big.NewInt(-1),
		big.NewInt(0),
		big.NewInt(1),
		big.NewInt(1_000_000),
		new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
	}
	var prev []byte
	for i, v := range values {
		enc := EncodeBigInt(v)
		if prev != nil && bytes.Compare(prev, enc) >= 0 {
			t.Errorf("BigInt sort order violated at index %d (%s)", i, v)
		}
		prev = enc
	}
}

func TestEnumValue(t *testing.T) {
	vals := []string{"transfer", "swap", "mint", "burn"}
	for i, v := range vals {
		enc, ok := EncodeEnumValue(vals, v)
		if !ok {
			t.Errorf("EncodeEnumValue(%q) not found", v)
		}
		if len(enc) != 1 || enc[0] != byte(i) {
			t.Errorf("EncodeEnumValue(%q) = %x, want [%02x]", v, enc, i)
		}
	}
	_, ok := EncodeEnumValue(vals, "invalid")
	if ok {
		t.Error("EncodeEnumValue(invalid) should return false")
	}
}
