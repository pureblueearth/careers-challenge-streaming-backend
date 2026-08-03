package agg

import (
	"math"
	"sort"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// referenceNow is a fixed "now" so tests are deterministic.
var referenceNow = time.Unix(1_700_000_000, 0).UTC()

func lessPresence(left, right PresenceChange) bool {
	if left.Time.Equal(right.Time) {
		return left.Seq < right.Seq
	}
	return left.Time.Before(right.Time)
}

// genPresence draws a set of presence changes for one room. Seq is the index,
// so (Time, Seq) is always unique and ties break deterministically.
func genPresence(t *rapid.T) []PresenceChange {
	count := rapid.IntRange(0, 40).Draw(t, "count")
	changes := make([]PresenceChange, count)
	for i := 0; i < count; i++ {
		offset := rapid.IntRange(1, 3599).Draw(t, "offset")
		changes[i] = PresenceChange{
			RoomID: "room_0",
			Time:   referenceNow.Add(-time.Duration(offset) * time.Second),
			Seq:    int64(i),
			InRoom: rapid.Bool().Draw(t, "inRoom"),
		}
	}
	return changes
}

func applyRooms(changes []PresenceChange) *Rooms {
	rooms := NewRooms()
	for _, change := range changes {
		rooms.Apply(change)
	}
	return rooms
}

// refOccupied is an independent reference: it samples each second of the window
// and asks what the state was, rather than integrating analytically.
func refOccupied(changes []PresenceChange, now time.Time, window time.Duration) float64 {
	sorted := append([]PresenceChange{}, changes...)
	sort.SliceStable(sorted, func(i, j int) bool { return lessPresence(sorted[i], sorted[j]) })

	windowStart := now.Add(-window)
	windowSeconds := int(window.Seconds())
	occupiedSeconds, index := 0, 0
	occupied := false
	for offset := 0; offset < windowSeconds; offset++ {
		second := windowStart.Add(time.Duration(offset) * time.Second)
		for index < len(sorted) && !sorted[index].Time.After(second) {
			occupied = sorted[index].InRoom
			index++
		}
		if occupied {
			occupiedSeconds++
		}
	}
	return float64(occupiedSeconds) / float64(windowSeconds)
}

var windows = []time.Duration{time.Minute, 5 * time.Minute, time.Hour}

func TestRoomOrderIndependent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		changes := genPresence(t)

		sorted := append([]PresenceChange{}, changes...)
		sort.SliceStable(sorted, func(i, j int) bool { return lessPresence(sorted[i], sorted[j]) })
		reversed := append([]PresenceChange{}, sorted...)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}

		for _, window := range windows {
			fromGenerated := applyRooms(changes).Occupancy("room_0", referenceNow, window)
			fromSorted := applyRooms(sorted).Occupancy("room_0", referenceNow, window)
			fromReversed := applyRooms(reversed).Occupancy("room_0", referenceNow, window)
			if fromGenerated != fromSorted || fromGenerated != fromReversed {
				t.Fatalf("order dependent (window=%v): %+v / %+v / %+v",
					window, fromGenerated, fromSorted, fromReversed)
			}
		}
	})
}

func TestRoomMatchesReference(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		changes := genPresence(t)
		for _, window := range windows {
			got := applyRooms(changes).Occupancy("room_0", referenceNow, window)
			want := refOccupied(changes, referenceNow, window)
			if math.Abs(got.OccupiedPct-want) > 1e-9 {
				t.Fatalf("occupied_pct=%v want=%v (window=%v)", got.OccupiedPct, want, window)
			}
			if got.OccupiedPct < 0 || got.OccupiedPct > 1 {
				t.Fatalf("occupied_pct out of range: %v", got.OccupiedPct)
			}
		}
	})
}

func TestRoomEmpty(t *testing.T) {
	got := NewRooms().Occupancy("nope", referenceNow, time.Minute)
	if got.InRoom || got.OccupiedPct != 0 {
		t.Fatalf("empty room not zero: %+v", got)
	}
}
