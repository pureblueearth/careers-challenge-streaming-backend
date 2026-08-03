package ingest

import (
	"testing"
	"time"

	"github.com/iskynsn/teton-streaming-backend/internal/domain"
	"pgregory.net/rapid"
)

var referenceNow = time.Unix(1_700_000_000, 0).UTC()

func base(eventType domain.EventType, eventTime time.Time) domain.Event {
	event := domain.Event{
		DeviceID: "dev_1", RoomID: "room_1", Type: eventType,
		Timestamp: eventTime.UTC().Format(time.RFC3339Nano), Seq: 1,
	}
	if eventType == domain.Presence {
		inRoom := true
		event.InRoom = &inRoom
	}
	return event
}

func TestClockBoundsAccept(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		offsetSeconds := rapid.IntRange(-3600, 3600).Draw(t, "offsetSeconds")
		event := base(domain.Heartbeat, referenceNow.Add(time.Duration(offsetSeconds)*time.Second))
		if _, reason := Validate(&event, referenceNow); reason != OK {
			t.Fatalf("in-bounds event rejected (%ds): %v", offsetSeconds, reason)
		}
	})
}

func TestClockBoundsReject(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		offsetSeconds := rapid.IntRange(3601, 1_000_000).Draw(t, "offsetSeconds")
		future := base(domain.Heartbeat, referenceNow.Add(time.Duration(offsetSeconds)*time.Second))
		if _, reason := Validate(&future, referenceNow); reason != FutureClock {
			t.Fatalf("want FutureClock, got %v", reason)
		}
		past := base(domain.Heartbeat, referenceNow.Add(-time.Duration(offsetSeconds)*time.Second))
		if _, reason := Validate(&past, referenceNow); reason != TooOld {
			t.Fatalf("want TooOld, got %v", reason)
		}
	})
}

// A possible fall must never be dropped for missing confidence (fail-safe).
func TestFallNeverDroppedForMissingConfidence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		offsetSeconds := rapid.IntRange(-3600, 3600).Draw(t, "offsetSeconds")
		event := base(domain.FallWarn, referenceNow.Add(time.Duration(offsetSeconds)*time.Second))
		event.Confidence = nil
		if _, reason := Validate(&event, referenceNow); reason != OK {
			t.Fatalf("fall dropped for missing confidence: %v", reason)
		}
	})
}

func TestValidateExamples(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*domain.Event)
		want   RejectReason
	}{
		{"ok", func(*domain.Event) {}, OK},
		{"missing device", func(event *domain.Event) { event.DeviceID = "" }, MissingField},
		{"missing room", func(event *domain.Event) { event.RoomID = "" }, MissingField},
		{"unknown type", func(event *domain.Event) { event.Type = "bogus" }, UnknownType},
		{"bad ts", func(event *domain.Event) { event.Timestamp = "not-a-time" }, BadTimestamp},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			event := base(domain.Heartbeat, referenceNow)
			testCase.mutate(&event)
			if _, reason := Validate(&event, referenceNow); reason != testCase.want {
				t.Fatalf("got %v, want %v", reason, testCase.want)
			}
		})
	}

	// presence with no in_room is dropped
	event := base(domain.Presence, referenceNow)
	event.InRoom = nil
	if _, reason := Validate(&event, referenceNow); reason != MissingPayload {
		t.Fatalf("presence without in_room: got %v, want MissingPayload", reason)
	}
}
