package set

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"math/bits"
	"time"

	"github.com/tulpenhaendler/dict/codec"
)

// EncodeKey encodes a uint64 as a length-prefixed big-endian byte slice.
// Preserves sort order: shorter encodings sort before longer ones.
func EncodeKey(v uint64) []byte {
	if v == 0 {
		return []byte{1, 0}
	}
	n := (bits.Len64(v) + 7) / 8
	buf := make([]byte, n+1)
	buf[0] = byte(n)
	for i := n; i > 0; i-- {
		buf[i] = byte(v)
		v >>= 8
	}
	return buf
}

// EncodeKeyTo appends a length-prefixed big-endian encoding of v to dst.
func EncodeKeyTo(dst []byte, v uint64) []byte {
	if v == 0 {
		return append(dst, 1, 0)
	}
	n := (bits.Len64(v) + 7) / 8
	dst = append(dst, byte(n))
	for i := n - 1; i >= 0; i-- {
		dst = append(dst, byte(v>>(uint(i)*8)))
	}
	return dst
}

// DecodeKey decodes a length-prefixed big-endian byte slice back to uint64.
func DecodeKey(b []byte) uint64 {
	if len(b) < 2 {
		return 0
	}
	n := int(b[0])
	if len(b) < n+1 {
		return 0
	}
	var v uint64
	for i := 1; i <= n; i++ {
		v = (v << 8) | uint64(b[i])
	}
	return v
}

// EncodeEnumValue encodes an enum string to its index byte.
func EncodeEnumValue(vals []string, s string) ([]byte, bool) {
	for i, v := range vals {
		if v == s {
			return []byte{byte(i)}, true
		}
	}
	return nil, false
}

// EncodeBigInt encodes a *big.Int in sort-preserving order.
// Sign byte: 0x00=negative, 0x01=zero, 0x02=positive.
// Negative magnitudes are ones-complemented for correct ordering.
func EncodeBigInt(v *big.Int) []byte {
	if v.Sign() == 0 {
		return []byte{0x01, 0}
	}

	magnitude := v.Bytes() // big-endian, no leading zeros

	if v.Sign() > 0 {
		buf := make([]byte, 2+len(magnitude))
		buf[0] = 0x02
		buf[1] = byte(len(magnitude))
		copy(buf[2:], magnitude)
		return buf
	}

	// Negative: ones-complement magnitude for correct sort order.
	// Longer (larger absolute value) should sort first (more negative),
	// so we invert the length byte too.
	buf := make([]byte, 2+len(magnitude))
	buf[0] = 0x00
	buf[1] = ^byte(len(magnitude))
	for i, b := range magnitude {
		buf[2+i] = ^b
	}
	return buf
}

// DecodeBigInt decodes a big.Int from the EncodeBigInt format.
func DecodeBigInt(b []byte) *big.Int {
	if len(b) < 2 {
		return new(big.Int)
	}
	sign := b[0]
	if sign == 0x01 {
		return new(big.Int)
	}

	var magLen int
	if sign == 0x02 {
		magLen = int(b[1])
	} else {
		magLen = int(^b[1])
	}

	if len(b) < 2+magLen {
		return new(big.Int)
	}

	mag := make([]byte, magLen)
	if sign == 0x02 {
		copy(mag, b[2:2+magLen])
	} else {
		for i := 0; i < magLen; i++ {
			mag[i] = ^b[2+i]
		}
	}

	v := new(big.Int).SetBytes(mag)
	if sign == 0x00 {
		v.Neg(v)
	}
	return v
}

// NextValueKey returns the smallest key that sorts after all composite keys
// for the given value key. Used as exclusive upper bound in range queries.
func NextValueKey(key []byte) []byte {
	// Increment the last byte; if it overflows, carry.
	next := make([]byte, len(key))
	copy(next, key)
	for i := len(next) - 1; i >= 0; i-- {
		next[i]++
		if next[i] != 0 {
			return next
		}
	}
	// All 0xFF — append a byte.
	return append(next, 0)
}

// EncodeInt64Key encodes an int64 for range queries (flip sign bit).
func EncodeInt64Key(v int64) []byte {
	return EncodeKey(uint64(v) ^ (1 << 63))
}

// DecodeInt64Key decodes an int64 from the flipped encoding.
func DecodeInt64Key(b []byte) int64 {
	return int64(DecodeKey(b) ^ (1 << 63))
}

// EncodeStoredValue encodes a Go value for a stored field.
func EncodeStoredValue(ft FieldType, v any) ([]byte, error) {
	switch ft.StoredAs {
	case StoredAsRange:
		var buf [8]byte
		switch ft.RangeEnc {
		case Uint64BE:
			binary.BigEndian.PutUint64(buf[:], v.(uint64))
		case Int64BE:
			binary.BigEndian.PutUint64(buf[:], uint64(v.(int64)))
		case Timestamp:
			binary.BigEndian.PutUint64(buf[:], uint64(v.(time.Time).UnixNano()))
		}
		return buf[:], nil
	case StoredAsString:
		c := codec.Get(ft.DictKey)
		if c == nil {
			return []byte(v.(string)), nil
		}
		return c.Encode(v.(string))
	}
	return nil, fmt.Errorf("unknown stored kind %d", ft.StoredAs)
}

// DecodeStoredValue decodes bytes back to the Go value for a stored field.
func DecodeStoredValue(ft FieldType, b []byte) (any, error) {
	switch ft.StoredAs {
	case StoredAsRange:
		if len(b) < 8 {
			return nil, fmt.Errorf("stored value too short: %d", len(b))
		}
		switch ft.RangeEnc {
		case Uint64BE:
			return binary.BigEndian.Uint64(b), nil
		case Int64BE:
			return int64(binary.BigEndian.Uint64(b)), nil
		case Timestamp:
			return time.Unix(0, int64(binary.BigEndian.Uint64(b))), nil
		}
	case StoredAsString:
		c := codec.Get(ft.DictKey)
		if c == nil {
			return string(b), nil
		}
		return c.Decode(b)
	}
	return nil, fmt.Errorf("unknown stored kind %d", ft.StoredAs)
}
