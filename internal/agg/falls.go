package agg

import (
	"crypto/sha1"
	"encoding/hex"
	"sync"
	"time"

	"github.com/iskynsn/teton-streaming-backend/internal/domain"
)

// Alarm is one distinct fall, the shape returned by /alarms and the live feed.
type Alarm struct {
	EventID    string  `json:"event_id"`
	DeviceID   string  `json:"device_id"`
	RoomID     string  `json:"room_id"`
	Timestamp  string  `json:"ts"`
	Confidence float64 `json:"confidence"`
}

// Falls deduplicates fall warnings. The generator may send the same fall 1-3
// times (same device_id and ts, different seq), so the key is (device_id, ts).
type Falls struct {
	mutex sync.Mutex
	seen  map[string]time.Time // dedup key -> event time, for pruning
}

func NewFalls() *Falls { return &Falls{seen: make(map[string]time.Time)} }

// Observe reports a fall warning. It returns the alarm and true the first time a
// given fall is seen, or false for a duplicate. A missing confidence defaults
// to 1.0 so a possible fall always alarms.
func (falls *Falls) Observe(event *domain.Event, eventTime time.Time) (Alarm, bool) {
	key := event.DeviceID + "|" + event.Timestamp

	falls.mutex.Lock()
	defer falls.mutex.Unlock()
	if _, duplicate := falls.seen[key]; duplicate {
		return Alarm{}, false
	}
	falls.seen[key] = eventTime

	confidence := 1.0
	if event.Confidence != nil {
		confidence = *event.Confidence
	}
	return Alarm{
		EventID:    eventID(key),
		DeviceID:   event.DeviceID,
		RoomID:     event.RoomID,
		Timestamp:  event.Timestamp,
		Confidence: confidence,
	}, true
}

// eventID is a stable id derived from the dedup key, so the same fall always
// gets the same id (even when replayed after a restart).
func eventID(key string) string {
	digest := sha1.Sum([]byte(key))
	return hex.EncodeToString(digest[:8])
}
