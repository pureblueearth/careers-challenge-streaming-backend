// Package agg builds the live answers (device health, room occupancy) from the
// stream of events. Everything here is keyed by event time, not arrival time,
// so out-of-order and late events produce the same result.
package agg

import (
	"sort"
	"sync"
	"time"
)

// RoomOccupancy is the answer to one occupancy query.
type RoomOccupancy struct {
	InRoom      bool    `json:"in_room"`
	OccupiedPct float64 `json:"occupied_pct"`
}

// point is one presence state change. Points are kept sorted by (eventTime, seq).
type point struct {
	eventTime time.Time
	seq       int64
	inRoom    bool
}

type roomState struct {
	points []point
}

// Rooms holds every room's presence timeline.
type Rooms struct {
	mutex sync.RWMutex
	rooms map[string]*roomState
}

func NewRooms() *Rooms { return &Rooms{rooms: make(map[string]*roomState)} }

// PresenceChange is one presence reading for a room, passed as a single named
// object so callers don't juggle positional arguments.
type PresenceChange struct {
	RoomID string
	Time   time.Time
	Seq    int64
	InRoom bool
}

// Apply records a presence change. Safe to call with events in any order.
func (rooms *Rooms) Apply(change PresenceChange) {
	rooms.mutex.Lock()
	defer rooms.mutex.Unlock()
	state := rooms.rooms[change.RoomID]
	if state == nil {
		state = &roomState{}
		rooms.rooms[change.RoomID] = state
	}
	state.insert(point{eventTime: change.Time, seq: change.Seq, inRoom: change.InRoom})
}

func (state *roomState) insert(newPoint point) {
	insertAt := sort.Search(len(state.points), func(i int) bool {
		existing := state.points[i]
		if existing.eventTime.Equal(newPoint.eventTime) {
			return existing.seq >= newPoint.seq
		}
		return existing.eventTime.After(newPoint.eventTime)
	})
	if insertAt < len(state.points) &&
		state.points[insertAt].eventTime.Equal(newPoint.eventTime) &&
		state.points[insertAt].seq == newPoint.seq {
		state.points[insertAt] = newPoint // exact duplicate, replace
		return
	}
	state.points = append(state.points, point{})
	copy(state.points[insertAt+1:], state.points[insertAt:])
	state.points[insertAt] = newPoint
}

// Occupancy returns the current state and the fraction of the window the room
// was occupied. An unknown room reads as empty/zero.
func (rooms *Rooms) Occupancy(roomID string, now time.Time, window time.Duration) RoomOccupancy {
	rooms.mutex.RLock()
	defer rooms.mutex.RUnlock()
	state := rooms.rooms[roomID]
	if state == nil || len(state.points) == 0 {
		return RoomOccupancy{}
	}
	return RoomOccupancy{
		InRoom:      state.points[len(state.points)-1].inRoom, // latest event wins
		OccupiedPct: state.occupiedPct(now, window),
	}
}

func (state *roomState) occupiedPct(now time.Time, window time.Duration) float64 {
	windowStart := now.Add(-window)

	// Carry the state as it was entering the window: the last change at or
	// before windowStart. Before any known change we assume not occupied.
	occupied := false
	index := 0
	for ; index < len(state.points) && !state.points[index].eventTime.After(windowStart); index++ {
		occupied = state.points[index].inRoom
	}

	var occupiedDuration time.Duration
	cursor := windowStart
	for ; index < len(state.points); index++ {
		change := state.points[index]
		if change.eventTime.After(now) {
			break
		}
		if occupied {
			occupiedDuration += change.eventTime.Sub(cursor)
		}
		cursor, occupied = change.eventTime, change.inRoom
	}
	if occupied {
		occupiedDuration += now.Sub(cursor)
	}

	fraction := float64(occupiedDuration) / float64(window)
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	return fraction
}
