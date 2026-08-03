// Package ingest accepts events over HTTP, checks them, and hands the good ones
// to the rest of the system.
package ingest

import (
	"time"

	"github.com/iskynsn/teton-streaming-backend/internal/domain"
)

// RejectReason says why an event was rejected. We keep it as a value so logs
// and metrics can group drops by cause. An empty reason means "accepted".
type RejectReason string

const (
	OK             RejectReason = ""
	MissingField   RejectReason = "missing_field"
	UnknownType    RejectReason = "unknown_type"
	BadTimestamp   RejectReason = "bad_timestamp"
	FutureClock    RejectReason = "future_clock"
	TooOld         RejectReason = "too_old"
	MissingPayload RejectReason = "missing_payload"
)

// We trust the device clock for ordering but bound how far off it can be:
// reject anything more than an hour ahead (broken clock) and accept up to an
// hour behind (a device replaying its offline buffer).
const (
	maxFuture = time.Hour
	maxPast   = time.Hour
)

// Validate checks one event against the acceptance rules. It returns the parsed
// timestamp so callers do not have to parse it again, and a reason. A reason of
// OK means the event is accepted.
func Validate(event *domain.Event, now time.Time) (time.Time, RejectReason) {
	if event.DeviceID == "" || event.RoomID == "" || event.Type == "" || event.Timestamp == "" {
		return time.Time{}, MissingField
	}
	if !event.Type.Valid() {
		return time.Time{}, UnknownType
	}
	eventTime, err := event.Time()
	if err != nil {
		return time.Time{}, BadTimestamp
	}
	if eventTime.After(now.Add(maxFuture)) {
		return time.Time{}, FutureClock
	}
	if eventTime.Before(now.Add(-maxPast)) {
		return time.Time{}, TooOld
	}

	if event.Type == domain.Presence && event.InRoom == nil {
		return time.Time{}, MissingPayload
	}

	return eventTime, OK
}
