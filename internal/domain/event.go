// Package domain holds the event type shared by ingest, the log, and the
// aggregators, so every layer agrees on one shape.
package domain

import "time"

// EventType is the "type" field saying what kind of event this is.
type EventType string

const (
	Heartbeat  EventType = "heartbeat"
	Presence   EventType = "presence"
	Motion     EventType = "motion"
	SleepState EventType = "sleep_state"
	FallWarn   EventType = "fall_warn"
	NetStatus  EventType = "net_status"
)

// known lists the types we accept. Anything else is bad input.
var known = map[EventType]bool{
	Heartbeat: true, Presence: true, Motion: true,
	SleepState: true, FallWarn: true, NetStatus: true,
}

func (eventType EventType) Valid() bool { return known[eventType] }

// Event is one event off the wire: the shared fields every event carries, plus
// the per-type fields only some events have.
//
// Per-type fields are pointers so we can tell "not sent" from "sent as zero".
// A presence event with in_room=false must not look like a missing field.
type Event struct {
	DeviceID  string    `json:"device_id"`
	RoomID    string    `json:"room_id"`
	Type      EventType `json:"type"`
	Timestamp string    `json:"ts"`  // device clock, e.g. 2026-05-23T18:53:49.123Z
	Seq       int64     `json:"seq"` // counts up per device; we order by ts, not seq

	InRoom     *bool    `json:"in_room,omitempty"`    // presence
	Magnitude  *float64 `json:"magnitude,omitempty"`  // motion, 0..1
	State      *string  `json:"state,omitempty"`      // sleep_state: asleep|awake|unknown
	Confidence *float64 `json:"confidence,omitempty"` // fall_warn, 0..1
	RSSI       *int     `json:"rssi,omitempty"`       // net_status, signal strength in dBm
}

// Time reads the device timestamp into a real time value. Go's RFC3339 parser
// handles the milliseconds the generator adds on its own.
func (event *Event) Time() (time.Time, error) {
	return time.Parse(time.RFC3339, event.Timestamp)
}
