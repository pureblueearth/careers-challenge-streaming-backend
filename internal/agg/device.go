package agg

import (
	"sync"
	"time"
)

const availWindow = 5 * time.Minute

// DeviceHealth is the answer to one health query.
type DeviceHealth struct {
	LastHeartbeatTS string  `json:"last_heartbeat_ts"`
	Availability5m  float64 `json:"availability_5m"`
}

type deviceState struct {
	lastHeartbeat time.Time
	beats         map[int64]struct{} // unix second that saw at least one heartbeat
}

// Devices holds every device's heartbeat history.
type Devices struct {
	mutex   sync.Mutex
	devices map[string]*deviceState
}

func NewDevices() *Devices { return &Devices{devices: make(map[string]*deviceState)} }

// Apply records one heartbeat. Safe to call with events in any order.
func (devices *Devices) Apply(deviceID string, eventTime time.Time) {
	devices.mutex.Lock()
	defer devices.mutex.Unlock()
	state := devices.devices[deviceID]
	if state == nil {
		state = &deviceState{beats: make(map[int64]struct{})}
		devices.devices[deviceID] = state
	}
	if eventTime.After(state.lastHeartbeat) {
		state.lastHeartbeat = eventTime
	}
	state.beats[eventTime.Unix()] = struct{}{}
}

// Health returns the latest heartbeat and the fraction of the last 5 minutes
// that had at least one heartbeat. Old buckets are pruned here.
func (devices *Devices) Health(deviceID string, now time.Time) DeviceHealth {
	devices.mutex.Lock()
	defer devices.mutex.Unlock()
	state := devices.devices[deviceID]
	if state == nil {
		return DeviceHealth{}
	}

	cutoff := now.Add(-availWindow).Unix()
	covered := 0
	for second := range state.beats {
		if second < cutoff {
			delete(state.beats, second)
			continue
		}
		if second <= now.Unix() {
			covered++
		}
	}

	fraction := float64(covered) / availWindow.Seconds()
	if fraction > 1 {
		fraction = 1
	}

	health := DeviceHealth{Availability5m: fraction}
	if !state.lastHeartbeat.IsZero() {
		health.LastHeartbeatTS = state.lastHeartbeat.UTC().Format(time.RFC3339Nano)
	}
	return health
}
