package ingest

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/iskynsn/teton-streaming-backend/internal/domain"
	"github.com/iskynsn/teton-streaming-backend/internal/obs"
)

// Publisher is the subset of the MQTT bus the gateway needs.
type Publisher interface {
	Publish(topic string, payload []byte)
}

// Gateway is the HTTP front door. It keeps the accept path cheap: parse,
// validate, publish the raw bytes, return 202. Heavy work happens downstream.
type Gateway struct {
	publisher Publisher
	metrics   *obs.Metrics
	now       func() time.Time // injectable so tests control the clock
}

func NewGateway(publisher Publisher, metrics *obs.Metrics) *Gateway {
	return &Gateway{publisher: publisher, metrics: metrics, now: time.Now}
}

// Topic routes an event by type first, so subscribers can pick out fall_warn on
// its own connection: events/<type>/<room>/<device>.
func Topic(event *domain.Event) string {
	return "events/" + string(event.Type) + "/" + event.RoomID + "/" + event.DeviceID
}

func (gateway *Gateway) HandleEvents(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, "read error", http.StatusBadRequest)
		return
	}

	var event domain.Event
	if err := json.Unmarshal(body, &event); err != nil {
		gateway.metrics.Drop("bad_json")
		http.Error(writer, "bad json", http.StatusBadRequest)
		return
	}

	// A well-formed request that we choose not to aggregate (e.g. a broken
	// clock) is still a 202: we received it, we dropped it on purpose.
	if _, reason := Validate(&event, gateway.now()); reason != OK {
		gateway.metrics.Drop(string(reason))
		writer.WriteHeader(http.StatusAccepted)
		return
	}

	// Publish the original bytes so no field is lost downstream.
	gateway.publisher.Publish(Topic(&event), body)
	gateway.metrics.Accept()

	writer.WriteHeader(http.StatusAccepted)
	_, _ = writer.Write([]byte(`{"ok":true}`))
}
