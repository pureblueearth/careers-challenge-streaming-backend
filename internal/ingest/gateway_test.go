package ingest

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iskynsn/teton-streaming-backend/internal/obs"
)

type capturedPublish struct {
	topic   string
	payload []byte
}

type fakePublisher struct {
	published []capturedPublish
}

func (fake *fakePublisher) Publish(topic string, payload []byte) {
	fake.published = append(fake.published, capturedPublish{topic, payload})
}

func newTestGateway() (*Gateway, *fakePublisher) {
	publisher := &fakePublisher{}
	gateway := NewGateway(publisher, obs.New(slog.Default()))
	gateway.now = func() time.Time { return referenceNow }
	return gateway, publisher
}

func post(gateway *Gateway, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	gateway.HandleEvents(recorder, request)
	return recorder
}

func TestGatewayPublishesValidEvent(t *testing.T) {
	gateway, publisher := newTestGateway()
	body := `{"device_id":"dev_1","room_id":"room_2","type":"heartbeat","ts":"` +
		referenceNow.UTC().Format(time.RFC3339Nano) + `","seq":1}`

	recorder := post(gateway, body)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", recorder.Code)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(publisher.published))
	}
	if got := publisher.published[0].topic; got != "events/heartbeat/room_2/dev_1" {
		t.Fatalf("topic = %q", got)
	}
	if string(publisher.published[0].payload) != body {
		t.Fatalf("payload was not the raw body")
	}
}

func TestGatewayDropsBrokenClockButAccepts(t *testing.T) {
	gateway, publisher := newTestGateway()
	future := referenceNow.Add(2 * time.Hour).UTC().Format(time.RFC3339Nano)
	body := `{"device_id":"dev_1","room_id":"room_2","type":"heartbeat","ts":"` + future + `","seq":1}`

	recorder := post(gateway, body)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", recorder.Code)
	}
	if len(publisher.published) != 0 {
		t.Fatalf("broken-clock event was published")
	}
}

func TestGatewayRejectsBadJSON(t *testing.T) {
	gateway, publisher := newTestGateway()

	recorder := post(gateway, `{not json`)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if len(publisher.published) != 0 {
		t.Fatalf("bad json was published")
	}
}
