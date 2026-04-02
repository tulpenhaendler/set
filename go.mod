module github.com/tulpenhaendler/set

go 1.24.0

require (
	github.com/RoaringBitmap/roaring/v2 v2.16.0
	github.com/tulpenhaendler/dict v0.0.0
)

require (
	github.com/bits-and-blooms/bitset v1.24.2 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/mattn/go-sqlite3 v1.14.38 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
)

replace github.com/tulpenhaendler/dict => ../dict
