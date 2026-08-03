package agg

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/iskynsn/teton-streaming-backend/internal/domain"
	"pgregory.net/rapid"
)

const (
	deviceCount = 5
	roomCount   = 3
)

// genEvents draws a mixed stream of heartbeats, presence changes, and falls.
// Heartbeats and presence reuse a small pool of devices/rooms so they
// aggregate; each fall gets a unique device so its (device, ts) key is
// unambiguous (two different falls must never collide on the dedup key).
func genEvents(t *rapid.T) []domain.Event {
	count := rapid.IntRange(0, 80).Draw(t, "count")
	events := make([]domain.Event, 0, count)
	sequence := int64(0)

	for i := 0; i < count; i++ {
		offset := rapid.IntRange(0, 3599).Draw(t, "offset")
		timestamp := referenceNow.Add(-time.Duration(offset) * time.Second).UTC().Format(time.RFC3339Nano)
		eventType := rapid.SampledFrom([]domain.EventType{
			domain.Heartbeat, domain.Presence, domain.FallWarn,
		}).Draw(t, "type")

		sequence++
		event := domain.Event{Type: eventType, Timestamp: timestamp, Seq: sequence}

		switch eventType {
		case domain.Heartbeat:
			event.DeviceID = fmt.Sprintf("dev_%d", rapid.IntRange(0, deviceCount-1).Draw(t, "device"))
			event.RoomID = "room_0"
		case domain.Presence:
			event.DeviceID = fmt.Sprintf("dev_%d", rapid.IntRange(0, deviceCount-1).Draw(t, "device"))
			event.RoomID = fmt.Sprintf("room_%d", rapid.IntRange(0, roomCount-1).Draw(t, "room"))
			inRoom := rapid.Bool().Draw(t, "inRoom")
			event.InRoom = &inRoom
		case domain.FallWarn:
			event.DeviceID = fmt.Sprintf("falldev_%d", i)
			event.RoomID = fmt.Sprintf("room_%d", rapid.IntRange(0, roomCount-1).Draw(t, "room"))
			confidence := rapid.Float64Range(0.7, 0.99).Draw(t, "confidence")
			event.Confidence = &confidence
			copies := rapid.IntRange(1, 3).Draw(t, "copies")
			for copyIndex := 1; copyIndex < copies; copyIndex++ {
				sequence++
				duplicate := event
				duplicate.Seq = sequence
				events = append(events, duplicate)
			}
		}
		events = append(events, event)
	}
	return events
}

func buildStore(events []domain.Event) *Store {
	store := NewStore()
	for i := range events {
		store.Apply(&events[i], mustTime(events[i]))
	}
	return store
}

// sameAnswers fails if the two stores disagree on any query the service serves.
func sameAnswers(t *rapid.T, left, right *Store) {
	for d := 0; d < deviceCount; d++ {
		id := fmt.Sprintf("dev_%d", d)
		if left.Devices.Health(id, referenceNow) != right.Devices.Health(id, referenceNow) {
			t.Fatalf("device %s health differs", id)
		}
	}
	for r := 0; r < roomCount; r++ {
		id := fmt.Sprintf("room_%d", r)
		for _, window := range windows {
			if left.Rooms.Occupancy(id, referenceNow, window) != right.Rooms.Occupancy(id, referenceNow, window) {
				t.Fatalf("room %s occupancy differs (window=%v)", id, window)
			}
		}
	}
	if !reflect.DeepEqual(left.AlarmsSince(time.Time{}), right.AlarmsSince(time.Time{})) {
		t.Fatalf("alarms differ")
	}
}

func TestStoreReplayEquivalent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		events := genEvents(t)

		live := buildStore(events) // events as they arrived

		replayed := append([]domain.Event{}, events...)
		sort.SliceStable(replayed, func(i, j int) bool {
			return mustTime(replayed[i]).Before(mustTime(replayed[j]))
		})
		sameAnswers(t, live, buildStore(replayed)) // events as the log replays them
	})
}
