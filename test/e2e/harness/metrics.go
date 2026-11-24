package harness

import (
	"bufio"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Metrics is one scrape of a /metrics endpoint (Prometheus text format).
type Metrics map[string]float64

// ScrapeMetrics fetches url and parses the samples. The key is the series as
// written (`name{label="x"}`, or `name` when it has no labels).
func ScrapeMetrics(url string) (Metrics, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	m := Metrics{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			continue
		}
		m[line[:i]] = v
	}
	return m, sc.Err()
}

// Sum adds every series of the metric name (all label sets), 0 when absent.
func (m Metrics) Sum(name string) float64 {
	var sum float64
	for series, v := range m {
		if series == name || strings.HasPrefix(series, name+"{") {
			sum += v
		}
	}
	return sum
}

// Has reports whether any series of the metric exists.
func (m Metrics) Has(name string) bool {
	for series := range m {
		if series == name || strings.HasPrefix(series, name+"{") {
			return true
		}
	}
	return false
}

// SumWhere adds the series of name whose label text contains every fragment
// (`consumer="analytics-aggregator"`).
func (m Metrics) SumWhere(name string, fragments ...string) float64 {
	var sum float64
next:
	for series, v := range m {
		if !strings.HasPrefix(series, name+"{") {
			continue
		}
		for _, f := range fragments {
			if !strings.Contains(series, f) {
				continue next
			}
		}
		sum += v
	}
	return sum
}
