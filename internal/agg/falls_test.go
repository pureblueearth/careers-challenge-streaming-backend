package agg

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/iskynsn/teton-streaming-backend/internal/domain"
	"pgregory.net/rapid"
)

// genFalls draws a set of distinct falls (one device each, so keys are unique)
// and emits each one 1-3 times with a different seq, mimicking sensor jitter.
// It returns the events plus the expected confidence per device.
func genFalls(t *rapid.T) ([]domain.Event, map[string]float64) {
	count := rapid.IntRange(0, 20).Draw(t, "count")
	var events []domain.Event
	expected := make(map[string]float64)
	sequence := int64(0)

	for i := 0; i < count; i++ {
		device := fmt.Sprintf("dev_%d", i)
		offset := rapid.IntRange(1, 3000).Draw(t, "offset")
		timestamp := referenceNow.Add(-time.Duration(offset) * time.Second).UTC().Format(time.RFC3339Nano)

		var confidence *float64
		expectedConfidence := 1.0 // fail-safe default when confidence is absent
		if rapid.Bool().Draw(t, "hasConfidence") {
			value := rapid.Float64Range(0.7, 0.99).Draw(t, "confidence")
			confidence = &value
			expectedConfidence = value
		}
		expected[device] = expectedConfidence

		copies := rapid.IntRange(1, 3).Draw(t, "copies")
		for copyIndex := 0; copyIndex < copies; copyIndex++ {
			sequence++
			events = append(events, domain.Event{
				DeviceID: device, RoomID: "room_0", Type: domain.FallWarn,
				Timestamp: timestamp, Seq: sequence, Confidence: confidence,
			})
		}
	}
	return events, expected
}

func permute(t *rapid.T, events []domain.Event) []domain.Event {
	if len(events) == 0 {
		return events
	}
	keys := rapid.SliceOfN(rapid.Float64(), len(events), len(events)).Draw(t, "perm")
	order := make([]int, len(events))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(left, right int) bool { return keys[order[left]] < keys[order[right]] })
	shuffled := make([]domain.Event, len(events))
	for position, original := range order {
		shuffled[position] = events[original]
	}
	return shuffled
}

func mustTime(event domain.Event) time.Time {
	eventTime, _ := event.Time()
	return eventTime
}

func TestFallsDedup(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		events, expected := genFalls(t)
		shuffled := permute(t, events)

		falls := NewFalls()
		alarms := make([]Alarm, 0, len(shuffled))
		for i := range shuffled {
			if alarm, isNew := falls.Observe(&shuffled[i], mustTime(shuffled[i])); isNew {
				alarms = append(alarms, alarm)
			}
		}

		// One alarm per distinct fall.
		if len(alarms) != len(expected) {
			t.Fatalf("got %d alarms, want %d distinct falls", len(alarms), len(expected))
		}

		seenIDs := make(map[string]bool)
		for _, alarm := range alarms {
			if alarm.Confidence != expected[alarm.DeviceID] {
				t.Fatalf("device %s confidence=%v want=%v", alarm.DeviceID, alarm.Confidence, expected[alarm.DeviceID])
			}
			if alarm.EventID == "" || seenIDs[alarm.EventID] {
				t.Fatalf("event_id not unique/present: %q", alarm.EventID)
			}
			seenIDs[alarm.EventID] = true
		}

		// Re-observing everything yields no new alarms (idempotent).
		for i := range shuffled {
			if _, isNew := falls.Observe(&shuffled[i], mustTime(shuffled[i])); isNew {
				t.Fatalf("duplicate fall produced a new alarm")
			}
		}
	})
}

func TestFallStableID(t *testing.T) {
	confidence := 0.9
	event := domain.Event{DeviceID: "dev_1", RoomID: "room_1", Type: domain.FallWarn,
		Timestamp: referenceNow.Format(time.RFC3339Nano), Seq: 1, Confidence: &confidence}

	firstAlarm, firstOK := NewFalls().Observe(&event, referenceNow)
	secondAlarm, secondOK := NewFalls().Observe(&event, referenceNow)
	if !firstOK || !secondOK || firstAlarm.EventID != secondAlarm.EventID {
		t.Fatalf("same fall produced different ids: %q vs %q", firstAlarm.EventID, secondAlarm.EventID)
	}
}
