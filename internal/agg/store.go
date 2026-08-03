package agg

import (
	"sort"
	"sync"
	"time"

	"github.com/iskynsn/teton-streaming-backend/internal/domain"
)

// Store ties the aggregators together and routes each event to the right one.
// Rebuilding a Store by replaying the event log restores all the live answers,
// which is how we recover after a restart.
type Store struct {
	Devices *Devices
	Rooms   *Rooms
	Falls   *Falls

	alarmsMutex sync.Mutex
	alarms      []alarmEntry
}

type alarmEntry struct {
	alarm     Alarm
	eventTime time.Time
}

func NewStore() *Store {
	return &Store{
		Devices: NewDevices(),
		Rooms:   NewRooms(),
		Falls:   NewFalls(),
	}
}

// Apply routes one already-validated event to its aggregator. For a fall it
// returns the alarm and true when the fall is seen for the first time, so the
// caller can push it to the live feed; duplicates and other types return false.
func (store *Store) Apply(event *domain.Event, eventTime time.Time) (Alarm, bool) {
	switch event.Type {
	case domain.Heartbeat:
		store.Devices.Apply(event.DeviceID, eventTime)
	case domain.Presence:
		if event.InRoom != nil {
			store.Rooms.Apply(PresenceChange{
				RoomID: event.RoomID,
				Time:   eventTime,
				Seq:    event.Seq,
				InRoom: *event.InRoom,
			})
		}
	case domain.FallWarn:
		if alarm, isNew := store.Falls.Observe(event, eventTime); isNew {
			store.alarmsMutex.Lock()
			store.alarms = append(store.alarms, alarmEntry{alarm: alarm, eventTime: eventTime})
			store.alarmsMutex.Unlock()
			return alarm, true
		}
	}
	return Alarm{}, false
}

// AlarmsSince returns every distinct alarm at or after the given time, ordered
// by event time then room then id so the result is deterministic.
func (store *Store) AlarmsSince(since time.Time) []Alarm {
	store.alarmsMutex.Lock()
	matching := make([]alarmEntry, 0, len(store.alarms))
	for _, entry := range store.alarms {
		if !entry.eventTime.Before(since) {
			matching = append(matching, entry)
		}
	}
	store.alarmsMutex.Unlock()

	sort.SliceStable(matching, func(i, j int) bool {
		left, right := matching[i], matching[j]
		if !left.eventTime.Equal(right.eventTime) {
			return left.eventTime.Before(right.eventTime)
		}
		if left.alarm.RoomID != right.alarm.RoomID {
			return left.alarm.RoomID < right.alarm.RoomID
		}
		return left.alarm.EventID < right.alarm.EventID
	})

	out := make([]Alarm, len(matching))
	for i, entry := range matching {
		out[i] = entry.alarm
	}
	return out
}
