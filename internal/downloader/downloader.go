package downloader

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/adshao/go-binance/v2"
)

// KlineDownloader is used to download K-line data from Binance
type KlineDownloader struct {
	client *binance.Client
}

// NewKlineDownloader creates a new downloader instance
func NewKlineDownloader() *KlineDownloader {
	return &KlineDownloader{
		client: binance.NewClient("", ""), // Public interfaces don't need an API key
	}
}

// DownloadKlines downloads 1-minute K-line data for a specified trading pair and time range, and saves it to a CSV file
// If the file already exists, the download will be skipped and the cached version will be used.
func (d *KlineDownloader) DownloadKlines(symbol, filePath string, startTime, endTime time.Time) error {
	// Check if the file already exists (cache)
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		fmt.Printf("Load data from cache: %s\n", filePath)
		return nil // The file already exists, just returning
	}

	fmt.Printf("开始下载 %s 从 %s 到 %s K-line data...\n", symbol, startTime.Format("2006-01-02"), endTime.Format("2006-01-02"))

	// --- Fix: Make sure the directory exists before creating the file ---
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("无法创建目录 %s: %v", dir, err)
	}
	// --- Repair finished ---

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("Can't create file %s: %v", filePath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	header := []string{"open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("Failed to write CSV header: %v", err)
	}

	for t := startTime; t.Before(endTime); {
		klines, err := d.client.NewKlinesService().
			Symbol(symbol).
			Interval("1m").
			StartTime(t.UnixMilli()).
			Limit(1000). // Binance allows a maximum of 1000 entries per request
			Do(context.Background())

		if err != nil {
			return fmt.Errorf("Failed to download K-line data: %v", err)
		}

		if len(klines) == 0 {
			break
		}

		for _, k := range klines {
			record := []string{
				fmt.Sprintf("%d", k.OpenTime),
				k.Open,
				k.High,
				k.Low,
				k.Close,
				k.Volume,
				fmt.Sprintf("%d", k.CloseTime),
				k.QuoteAssetVolume,
				fmt.Sprintf("%d", k.TradeNum),
				k.TakerBuyBaseAssetVolume,
				k.TakerBuyQuoteAssetVolume,
			}
			if err := writer.Write(record); err != nil {
				return fmt.Errorf("Failed to write CSV record: %v", err)
			}
		}

		// Update the start time for the next request
		t = time.UnixMilli(klines[len(klines)-1].CloseTime + 1)
		fmt.Printf("Data downloaded to %s\n", t.Format("2006-01-02 15:04:05"))
		time.Sleep(200 * time.Millisecond) // Avoid requesting too often
	}

	fmt.Printf("Successfully downloaded K-line data to %s\n", filePath)
	return nil
}
