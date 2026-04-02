package set

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/tulpenhaendler/dict"
)

// Store manages a shared dict and a set of repos.
type Store struct {
	dir  string
	dict *dict.Dict
	mu   sync.Mutex
	repos map[string]*Repo // schema hash → repo
}

// Open opens or creates an FST store at the given directory.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, "dict"), 0755); err != nil {
		return nil, fmt.Errorf("fst: mkdir dict: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "repos"), 0755); err != nil {
		return nil, fmt.Errorf("fst: mkdir repos: %w", err)
	}

	d, err := dict.Open(filepath.Join(dir, "dict", "dict"))
	if err != nil {
		return nil, fmt.Errorf("fst: open dict: %w", err)
	}

	return &Store{
		dir:   dir,
		dict:  d,
		repos: make(map[string]*Repo),
	}, nil
}

// Dict returns the shared dictionary.
func (s *Store) Dict() *dict.Dict {
	return s.dict
}

// Repo opens or creates a repository for the given schema.
func (s *Store) Repo(schema Schema) (*Repo, error) {
	hash := schema.Hash()

	s.mu.Lock()
	defer s.mu.Unlock()

	if r, ok := s.repos[hash]; ok {
		return r, nil
	}

	repoDir := filepath.Join(s.dir, "repos", hash)
	schemaPath := filepath.Join(repoDir, "schema.json")

	if _, err := os.Stat(repoDir); os.IsNotExist(err) {
		// New repo — create directory and write schema.
		if err := os.MkdirAll(filepath.Join(repoDir, "segments"), 0755); err != nil {
			return nil, fmt.Errorf("fst: mkdir repo: %w", err)
		}
		data, err := json.MarshalIndent(schema, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("fst: marshal schema: %w", err)
		}
		if err := os.WriteFile(schemaPath, data, 0644); err != nil {
			return nil, fmt.Errorf("fst: write schema: %w", err)
		}
	} else {
		// Existing repo — verify schema hash matches.
		data, err := os.ReadFile(schemaPath)
		if err != nil {
			return nil, fmt.Errorf("fst: read schema: %w", err)
		}
		var stored struct {
			Hash string `json:"hash"`
		}
		if err := json.Unmarshal(data, &stored); err != nil {
			return nil, fmt.Errorf("fst: parse schema: %w", err)
		}
		if stored.Hash != hash {
			return nil, fmt.Errorf("fst: schema hash mismatch: stored %s, got %s", stored.Hash, hash)
		}
	}

	r, err := openRepo(repoDir, schema, s.dict)
	if err != nil {
		return nil, err
	}

	s.repos[hash] = r
	return r, nil
}

// Intersect returns an iterator over the intersection of multiple query results.
func (s *Store) Intersect(iters ...*Iterator) *Iterator {
	if len(iters) == 0 {
		return &Iterator{bitmap: roaring64.New()}
	}
	result := iters[0].bitmap.Clone()
	for _, it := range iters[1:] {
		result.And(it.bitmap)
	}
	return newIterator(result)
}

// Union returns an iterator over the union of multiple query results.
func (s *Store) Union(iters ...*Iterator) *Iterator {
	if len(iters) == 0 {
		return &Iterator{bitmap: roaring64.New()}
	}
	result := iters[0].bitmap.Clone()
	for _, it := range iters[1:] {
		result.Or(it.bitmap)
	}
	return newIterator(result)
}

// Close syncs and closes the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.repos {
		r.close()
	}
	return s.dict.Close()
}
