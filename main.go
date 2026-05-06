package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	colorReset  = "\033[0m"
	colorBold   = "\033[1m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
)

const (
	pingURL      = "https://speed.cloudflare.com/__down?bytes=1"
	downloadURL  = "https://speed.cloudflare.com/__down?bytes=26214400"
	uploadURL    = "https://speed.cloudflare.com/__up"
	testDuration = 10 * time.Second
	numConns     = 4
	pingRounds   = 8
)

type countingReader struct {
	r     io.Reader
	total *int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		atomic.AddInt64(c.total, int64(n))
	}
	return n, err
}

func main() {
	fmt.Print("\033[H\033[2J") // clear screen
	printBanner()

	fmt.Printf("\n%s%s◆  LATENCY%s\n\n", colorBold, colorCyan, colorReset)
	lat, jitter := measureLatency()

	fmt.Printf("\n%s%s◆  DOWNLOAD%s\n\n", colorBold, colorCyan, colorReset)
	dl := measureSpeed("download")

	fmt.Printf("\n%s%s◆  UPLOAD%s\n\n", colorBold, colorCyan, colorReset)
	ul := measureSpeed("upload")

	printSummary(lat, jitter, dl, ul)
}

func printBanner() {
	fmt.Printf("%s%s", colorBold, colorCyan)
	fmt.Println("  ╔══════════════════════════════════════════════════════════╗")
	fmt.Println("  ║            SPEEDTEST                                     ║")
	fmt.Println("  ╚══════════════════════════════════════════════════════════╝")
	fmt.Printf("%s", colorReset)
}

func measureLatency() (float64, float64) {
	client := &http.Client{Timeout: 5 * time.Second}
	var latencies []float64

	for i := 0; i < pingRounds; i++ {
		start := time.Now()
		resp, err := client.Get(pingURL)
		if err != nil {
			fmt.Printf("  Ping %2d: erro\n", i+1)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		ms := float64(time.Since(start).Microseconds()) / 1000.0
		latencies = append(latencies, ms)
		fmt.Printf("  Ping %2d: %s%.1f ms%s\n", i+1, latencyColor(ms), ms, colorReset)
	}

	if len(latencies) == 0 {
		return 0, 0
	}

	var sum float64
	for _, l := range latencies {
		sum += l
	}
	avg := sum / float64(len(latencies))

	var jsum float64
	for _, l := range latencies {
		jsum += math.Abs(l - avg)
	}
	return avg, jsum / float64(len(latencies))
}

func measureSpeed(mode string) float64 {
	// testCtx is used directly in HTTP requests so cancelling it
	// interrupts in-flight Read/Write calls immediately.
	testCtx, testCancel := context.WithTimeout(context.Background(), testDuration)
	defer testCancel()

	var totalBytes int64
	var wg sync.WaitGroup

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if mode == "download" {
				runDownload(testCtx, &totalBytes)
			} else {
				runUpload(testCtx, &totalBytes)
			}
		}()
	}

	start := time.Now()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-testCtx.Done():
			wg.Wait()
			elapsed := time.Since(start).Seconds()
			total := atomic.LoadInt64(&totalBytes)
			fmt.Println()
			return float64(total*8) / elapsed / 1_000_000

		case <-ticker.C:
			elapsed := time.Since(start).Seconds()
			b := atomic.LoadInt64(&totalBytes)
			speed := float64(b*8) / elapsed / 1_000_000
			progress := math.Min(elapsed/testDuration.Seconds(), 1.0)
			printProgress(progress, speed, mode)
		}
	}
}

// runDownload streams data from Cloudflare until ctx is cancelled.
// Using ctx in the request means Read() returns immediately on cancellation.
func runDownload(ctx context.Context, totalBytes *int64) {
	client := &http.Client{}
	for ctx.Err() == nil {
		req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
		if err != nil {
			return
		}
		req.Header.Set("User-Agent", "speedtest-cli/1.0")

		resp, err := client.Do(req)
		if err != nil {
			return
		}

		buf := make([]byte, 32*1024)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				atomic.AddInt64(totalBytes, int64(n))
			}
			if err != nil {
				resp.Body.Close()
				break
			}
		}
	}
}

// runUpload sends random data to Cloudflare until ctx is cancelled.
func runUpload(ctx context.Context, totalBytes *int64) {
	client := &http.Client{}
	data := make([]byte, 4*1024*1024) // 4 MB chunks
	rand.Read(data)

	for ctx.Err() == nil {
		cr := &countingReader{r: bytes.NewReader(data), total: totalBytes}
		req, err := http.NewRequestWithContext(ctx, "POST", uploadURL, cr)
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("User-Agent", "speedtest-cli/1.0")
		req.ContentLength = int64(len(data))

		resp, err := client.Do(req)
		if err != nil {
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func printProgress(progress, speed float64, mode string) {
	width := 38
	filled := int(progress * float64(width))
	if filled > width {
		filled = width
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	pct := int(progress * 100)

	label := "⬇ Download"
	if mode == "upload" {
		label = "⬆ Upload  "
	}

	fmt.Printf("\r  %s  [%s%s%s] %3d%%  %s%.2f Mbps%s   ",
		label,
		speedColor(speed), bar, colorReset,
		pct,
		speedColor(speed), speed, colorReset,
	)
}

func printSummary(lat, jitter, dl, ul float64) {
	rating, ratingColor := speedRating(dl, ul, lat)
	hint := usageHint(dl)

	fmt.Printf("\n%s%s", colorBold, colorCyan)
	fmt.Println("  ╔══════════════════════════════════════════════════════════╗")
	fmt.Println("  ║                    Result                                ║")
	fmt.Println("  ╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("  ║  %-18s  %s%-36s%s║\n", "⏱  Latency:",
		latencyColor(lat), fmt.Sprintf("%.1f ms", lat), colorReset+colorBold+colorCyan)
	fmt.Printf("  ║  %-18s  %s%-36s%s║\n", "〰  Jitter:",
		latencyColor(jitter), fmt.Sprintf("%.1f ms", jitter), colorReset+colorBold+colorCyan)
	fmt.Println("  ╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("  ║  %-18s  %s%-36s%s║\n", "⬇  Download:",
		speedColor(dl), fmt.Sprintf("%.2f Mbps", dl), colorReset+colorBold+colorCyan)
	fmt.Printf("  ║  %-18s  %s%-36s%s║\n", "⬆  Upload:",
		speedColor(ul), fmt.Sprintf("%.2f Mbps", ul), colorReset+colorBold+colorCyan)
	fmt.Println("  ╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("  ║  %-18s  %s%-36s%s║\n", "★  Rating:",
		ratingColor, rating, colorReset+colorBold+colorCyan)
	fmt.Printf("  ║  %-20s %-36s  ║\n", "", hint)
	fmt.Println("  ╚══════════════════════════════════════════════════════════╝")
	fmt.Printf("%s\n", colorReset)
}

func speedRating(dl, ul, lat float64) (string, string) {
	switch {
	case dl >= 500 && ul >= 100 && lat < 10:
		return "🐰 Ultra-fast (Premium Fiber)", colorGreen
	case dl >= 100 && ul >= 20 && lat < 20:
		return "🐰 Excellent (Fiber)", colorGreen
	case dl >= 50 && ul >= 10 && lat < 50:
		return "👍 Very Good", colorGreen
	case dl >= 25 && ul >= 5 && lat < 80:
		return "👍 Good", colorYellow
	case dl >= 10 && ul >= 2 && lat < 150:
		return "Regular", colorYellow
	case dl >= 5:
		return "🐢 Slow", colorRed
	default:
		return "🐢 Very Slow", colorRed
	}
}

func usageHint(dl float64) string {
	switch {
	case dl >= 100:
		return "Ideal for 4K, cloud gaming, and video calls."
	case dl >= 25:
		return "Ideal for HD streaming and remote work."
	case dl >= 10:
		return "Suitable for streaming and video calls"
	case dl >= 5:
		return "Supports SD browsing and streaming."
	default:
		return "Limited — basic navigation only"
	}
}

func latencyColor(ms float64) string {
	switch {
	case ms < 20:
		return colorGreen
	case ms < 60:
		return colorYellow
	default:
		return colorRed
	}
}

func speedColor(mbps float64) string {
	switch {
	case mbps >= 50:
		return colorGreen
	case mbps >= 10:
		return colorYellow
	default:
		return colorRed
	}
}
