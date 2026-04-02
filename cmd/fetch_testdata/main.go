// Fetches ~500k Tezos transactions from TzKT API and saves as JSONL.
// Uses cursor-based pagination on the internal `id` field.
//
// Usage: go run ./cmd/fetch_testdata
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type Transaction struct {
	ID        int64  `json:"id"`
	Level     int64  `json:"level"`
	Timestamp string `json:"timestamp"`
	Hash      string `json:"hash"`
	Sender    Addr   `json:"sender"`
	Target    *Addr  `json:"target"`
	Amount    int64  `json:"amount"`
	BakerFee  int64  `json:"bakerFee"`
	GasUsed   int64  `json:"gasUsed"`
	Status    string `json:"status"`
}

type Addr struct {
	Address string `json:"address"`
}

const (
	batchSize = 10000
	target    = 500000
	baseURL   = "https://api.tzkt.io/v1/operations/transactions"
)

func main() {
	outDir := filepath.Join("test_data")
	os.MkdirAll(outDir, 0755)
	outPath := filepath.Join(outDir, "tezos_txs.jsonl")

	f, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	enc := json.NewEncoder(f)

	var lastID int64
	total := 0

	for total < target {
		url := fmt.Sprintf("%s?limit=%d&select=id,level,timestamp,hash,sender,target,amount,bakerFee,gasUsed,status&status=applied&sort.asc=id", baseURL, batchSize)
		if lastID > 0 {
			url += fmt.Sprintf("&offset.cr=%d", lastID)
		}

		fmt.Printf("Fetching batch %d-%d (lastID=%d)...\n", total, total+batchSize, lastID)

		resp, err := fetchWithRetry(url, 3)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fetch: %v\n", err)
			os.Exit(1)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "read body: %v\n", err)
			os.Exit(1)
		}

		var txs []Transaction
		if err := json.Unmarshal(body, &txs); err != nil {
			fmt.Fprintf(os.Stderr, "unmarshal: %v\n", err)
			os.Exit(1)
		}

		if len(txs) == 0 {
			fmt.Println("No more transactions.")
			break
		}

		for _, tx := range txs {
			if err := enc.Encode(tx); err != nil {
				fmt.Fprintf(os.Stderr, "encode: %v\n", err)
				os.Exit(1)
			}
		}

		lastID = txs[len(txs)-1].ID
		total += len(txs)
		fmt.Printf("  Got %d txs (total: %d, lastID: %d)\n", len(txs), total, lastID)

		// Respect rate limits.
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("Done. Wrote %d transactions to %s\n", total, outPath)
}

func fetchWithRetry(url string, retries int) (*http.Response, error) {
	for i := 0; i < retries; i++ {
		resp, err := http.Get(url)
		if err != nil {
			if i < retries-1 {
				time.Sleep(time.Duration(i+1) * time.Second)
				continue
			}
			return nil, err
		}
		if resp.StatusCode == 429 {
			resp.Body.Close()
			wait := time.Duration(i+1) * 5 * time.Second
			fmt.Printf("  Rate limited, waiting %s...\n", wait)
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
		}
		return resp, nil
	}
	return nil, fmt.Errorf("exhausted retries")
}
