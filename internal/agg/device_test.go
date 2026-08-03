package agg

import (
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// genHeartbeats draws heartbeat timestamps. Some land in the same second (and
// some outside the 5m window) on purpose, to exercise bucketing and pruning.
func genHeartbeats(t *rapid.T) []time.Time {
	count := rapid.IntRange(0, 60).Draw(t, "count")
	heartbeats := make([]time.Time, count)
	for i := 0; i < count; i++ {
		offset := rapid.IntRange(0, 400).Draw(t, "offset") // seconds before now
		millis := rapid.IntRange(0, 999).Draw(t, "millis") // sub-second jitter
		heartbeats[i] = referenceNow.Add(-time.Duration(offset)*time.Second + time.Duration(millis)*time.Millisecond)
	}
	return heartbeats
}

func applyDevices(heartbeats []time.Time) *Devices {
	devices := NewDevices()
	for _, eventTime := range heartbeats {
		devices.Apply("dev_0", eventTime)
	}
	return devices
}

// refAvailability counts distinct seconds with a heartbeat in [now-5m, now].
func refAvailability(heartbeats []time.Time, now time.Time) float64 {
	cutoff := now.Add(-availWindow).Unix()
	seconds := make(map[int64]bool)
	for _, heartbeat := range heartbeats {
		second := heartbeat.Unix()
		if second >= cutoff && second <= now.Unix() {
			seconds[second] = true
		}
	}
	fraction := float64(len(seconds)) / availWindow.Seconds()
	if fraction > 1 {
		fraction = 1
	}
	return fraction
}

func refLastHeartbeat(heartbeats []time.Time) string {
	if len(heartbeats) == 0 {
		return ""
	}
	latest := heartbeats[0]
	for _, heartbeat := range heartbeats[1:] {
		if heartbeat.After(latest) {
			latest = heartbeat
		}
	}
	return latest.UTC().Format(time.RFC3339Nano)
}

func TestDeviceOrderIndependent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		heartbeats := genHeartbeats(t)
		reversed := append([]time.Time{}, heartbeats...)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}
		forward := applyDevices(heartbeats).Health("dev_0", referenceNow)
		backward := applyDevices(reversed).Health("dev_0", referenceNow)
		if forward != backward {
			t.Fatalf("order dependent: %+v vs %+v", forward, backward)
		}
	})
}

func TestDeviceMatchesReference(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		heartbeats := genHeartbeats(t)
		got := applyDevices(heartbeats).Health("dev_0", referenceNow)
		if len(heartbeats) == 0 {
			if got != (DeviceHealth{}) {
				t.Fatalf("no heartbeats but got %+v", got)
			}
			return
		}
		if want := refAvailability(heartbeats, referenceNow); math.Abs(got.Availability5m-want) > 1e-9 {
			t.Fatalf("availability=%v want=%v", got.Availability5m, want)
		}
		if got.Availability5m < 0 || got.Availability5m > 1 {
			t.Fatalf("availability out of range: %v", got.Availability5m)
		}
		if want := refLastHeartbeat(heartbeats); got.LastHeartbeatTS != want {
			t.Fatalf("last_heartbeat=%q want=%q", got.LastHeartbeatTS, want)
		}
	})
}
