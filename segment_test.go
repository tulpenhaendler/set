package set

import (
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestSegmentBuildAndQuery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "seg001")
	fields := []Field{
		{Name: "color", Type: String(0)},
		{Name: "size", Type: Range(Uint64BE)},
	}

	sb := newSegmentBuilder(dir, fields)

	// Record 1: color=red, size=10
	sb.add("color", EncodeKey(100), 1) // 100 = pretend dict_id for "red"
	sb.add("size", EncodeKey(10), 1)

	// Record 2: color=red, size=20
	sb.add("color", EncodeKey(100), 2)
	sb.add("size", EncodeKey(20), 2)

	// Record 3: color=blue, size=30
	sb.add("color", EncodeKey(200), 3) // 200 = pretend dict_id for "blue"
	sb.add("size", EncodeKey(30), 3)

	// Record 4: color=blue, size=10
	sb.add("color", EncodeKey(200), 4)
	sb.add("size", EncodeKey(10), 4)

	if err := sb.build(); err != nil {
		t.Fatal(err)
	}

	// Open the segment.
	seg, err := openSegment(dir, []string{"color", "size"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.close()

	lookup := func(fi *fieldIndex, key []byte) *roaring64.Bitmap {
		bm := roaring64.New()
		fi.lookupEq(key, bm)
		return bm
	}

	lookupRange := func(fi *fieldIndex, from, to []byte) *roaring64.Bitmap {
		bm := roaring64.New()
		fi.lookupRange(from, to, bm)
		return bm
	}

	// Eq: color=red (dict_id=100) → records {1, 2}
	bm := lookup(seg.fields["color"], EncodeKey(100))
	if bm.GetCardinality() != 2 || !bm.Contains(1) || !bm.Contains(2) {
		t.Errorf("color=red: got %v, want {1, 2}", bm.ToArray())
	}

	// Eq: color=blue (dict_id=200) → records {3, 4}
	bm = lookup(seg.fields["color"], EncodeKey(200))
	if bm.GetCardinality() != 2 || !bm.Contains(3) || !bm.Contains(4) {
		t.Errorf("color=blue: got %v, want {3, 4}", bm.ToArray())
	}

	// Eq: size=10 → records {1, 4}
	bm = lookup(seg.fields["size"], EncodeKey(10))
	if bm.GetCardinality() != 2 || !bm.Contains(1) || !bm.Contains(4) {
		t.Errorf("size=10: got %v, want {1, 4}", bm.ToArray())
	}

	// Range: size >= 20 → records {2, 3}
	bm = lookupRange(seg.fields["size"], EncodeKey(20), nil)
	if bm.GetCardinality() != 2 || !bm.Contains(2) || !bm.Contains(3) {
		t.Errorf("size>=20: got %v, want {2, 3}", bm.ToArray())
	}

	// Intersection: color=red AND size=10 → record {1}
	bmColor := lookup(seg.fields["color"], EncodeKey(100))
	bmSize := lookup(seg.fields["size"], EncodeKey(10))
	bmColor.And(bmSize)
	if bmColor.GetCardinality() != 1 || !bmColor.Contains(1) {
		t.Errorf("color=red AND size=10: got %v, want {1}", bmColor.ToArray())
	}

	// Not found.
	bm = lookup(seg.fields["color"], EncodeKey(999))
	if bm.GetCardinality() != 0 {
		t.Errorf("color=999: got %v, want empty", bm.ToArray())
	}
}
