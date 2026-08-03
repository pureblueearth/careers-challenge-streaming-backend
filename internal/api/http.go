// Package api exposes the read endpoints and the live alarm feed over HTTP.
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/iskynsn/teton-streaming-backend/internal/agg"
	"github.com/iskynsn/teton-streaming-backend/internal/ingest"
	"github.com/iskynsn/teton-streaming-backend/internal/obs"
)

// Server wires the HTTP routes over the shared store, gateway, and alarm broker.
type Server struct {
	store   *agg.Store
	broker  *Broker
	gateway *ingest.Gateway
	metrics *obs.Metrics
	now     func() time.Time
}

func NewServer(store *agg.Store, broker *Broker, gateway *ingest.Gateway, metrics *obs.Metrics) *Server {
	return &Server{store: store, broker: broker, gateway: gateway, metrics: metrics, now: time.Now}
}

func (server *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", server.gateway.HandleEvents)
	mux.HandleFunc("GET /devices/{id}/health", server.handleHealth)
	mux.HandleFunc("GET /rooms/{id}/occupancy", server.handleOccupancy)
	mux.HandleFunc("GET /alarms", server.handleAlarms)
	mux.HandleFunc("GET /alarms/stream", server.handleAlarmStream)
	mux.HandleFunc("GET /metrics", server.metrics.Handler())
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("ok"))
	})
	return mux
}

func (server *Server) handleHealth(writer http.ResponseWriter, request *http.Request) {
	health := server.store.Devices.Health(request.PathValue("id"), server.now())
	writeJSON(writer, health)
}

func (server *Server) handleOccupancy(writer http.ResponseWriter, request *http.Request) {
	window := parseWindow(request.URL.Query().Get("window"))
	occupancy := server.store.Rooms.Occupancy(request.PathValue("id"), server.now(), window)
	writeJSON(writer, occupancy)
}

func (server *Server) handleAlarms(writer http.ResponseWriter, request *http.Request) {
	since := parseSince(request.URL.Query().Get("since"))
	writeJSON(writer, map[string]any{"alarms": server.store.AlarmsSince(since)})
}

// parseWindow maps the 1m|5m|1h query value to a duration, defaulting to 5m.
func parseWindow(value string) time.Duration {
	switch value {
	case "1m":
		return time.Minute
	case "1h":
		return time.Hour
	case "5m", "":
		return 5 * time.Minute
	}
	if parsed, err := time.ParseDuration(value); err == nil {
		return parsed
	}
	return 5 * time.Minute
}

// parseSince accepts an RFC3339 timestamp. Empty or "0" means "from the start".
func parseSince(value string) time.Time {
	if value == "" || value == "0" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed
	}
	return time.Time{}
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(value)
}
