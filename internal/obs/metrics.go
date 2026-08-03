// Package obs holds the service's counters and structured logger. It is small
// on purpose: enough to see ingest rate, drops by reason, and alarm volume.
package obs

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
)

type Metrics struct {
	Log *slog.Logger

	accepted atomic.Int64
	dropped  atomic.Int64
	alarms   atomic.Int64
	replayed atomic.Int64

	reasonMutex sync.Mutex
	byReason    map[string]int64
}

func New(logger *slog.Logger) *Metrics {
	return &Metrics{Log: logger, byReason: make(map[string]int64)}
}

func (metrics *Metrics) Accept() { metrics.accepted.Add(1) }
func (metrics *Metrics) Alarm()  { metrics.alarms.Add(1) }

func (metrics *Metrics) Replayed(count int64) { metrics.replayed.Add(count) }

// Drop records a dropped event and groups it by why.
func (metrics *Metrics) Drop(reason string) {
	metrics.dropped.Add(1)
	metrics.reasonMutex.Lock()
	metrics.byReason[reason]++
	metrics.reasonMutex.Unlock()
}

// Handler serves the counters as JSON at /metrics.
func (metrics *Metrics) Handler() http.HandlerFunc {
	return func(writer http.ResponseWriter, _ *http.Request) {
		metrics.reasonMutex.Lock()
		reasons := make(map[string]int64, len(metrics.byReason))
		for reason, count := range metrics.byReason {
			reasons[reason] = count
		}
		metrics.reasonMutex.Unlock()

		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"accepted":          metrics.accepted.Load(),
			"dropped":           metrics.dropped.Load(),
			"dropped_by_reason": reasons,
			"alarms":            metrics.alarms.Load(),
			"replayed":          metrics.replayed.Load(),
		})
	}
}
