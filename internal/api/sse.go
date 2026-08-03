package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/iskynsn/teton-streaming-backend/internal/agg"
)

// Broker fans confirmed alarms out to all connected feed subscribers.
type Broker struct {
	mutex       sync.Mutex
	subscribers map[chan agg.Alarm]struct{}
}

func NewBroker() *Broker {
	return &Broker{subscribers: make(map[chan agg.Alarm]struct{})}
}

func (broker *Broker) subscribe() (chan agg.Alarm, func()) {
	channel := make(chan agg.Alarm, 256)
	broker.mutex.Lock()
	broker.subscribers[channel] = struct{}{}
	broker.mutex.Unlock()

	unsubscribe := func() {
		broker.mutex.Lock()
		delete(broker.subscribers, channel)
		broker.mutex.Unlock()
	}
	return channel, unsubscribe
}

// Publish sends an alarm to every subscriber. A subscriber whose buffer is full
// is skipped, not blocked: it will catch up by replaying on its next reconnect.
func (broker *Broker) Publish(alarm agg.Alarm) {
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	for channel := range broker.subscribers {
		select {
		case channel <- alarm:
		default:
		}
	}
}

// handleAlarmStream is the SSE endpoint. It subscribes first, then replays the
// backlog from ?since, then streams live alarms. Subscribing before replaying
// means an alarm raised during the handoff is never lost; any overlap is
// deduped by the client using the stable event_id.
func (server *Server) handleAlarmStream(writer http.ResponseWriter, request *http.Request) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")

	channel, unsubscribe := server.broker.subscribe()
	defer unsubscribe()

	since := parseSince(request.URL.Query().Get("since"))
	for _, alarm := range server.store.AlarmsSince(since) {
		writeEvent(writer, alarm)
	}
	flusher.Flush()

	for {
		select {
		case <-request.Context().Done():
			return
		case alarm := <-channel:
			writeEvent(writer, alarm)
			flusher.Flush()
		}
	}
}

func writeEvent(writer http.ResponseWriter, alarm agg.Alarm) {
	payload, err := json.Marshal(alarm)
	if err != nil {
		return
	}
	fmt.Fprintf(writer, "id: %s\ndata: %s\n\n", alarm.EventID, payload)
}
