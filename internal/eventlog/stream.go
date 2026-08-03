// Package eventlog is the durable record of accepted events, backed by a Redis
// stream. We append every accepted event on the way in and replay the whole
// stream on startup to rebuild the in-memory views, which is how the service
// recovers after a restart.
package eventlog

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const streamKey = "events"

// Log appends events to and replays them from one Redis stream.
type Log struct {
	client    *redis.Client
	batchSize int64
}

func New(client *redis.Client) *Log {
	return &Log{client: client, batchSize: 1000}
}

// Append stores one raw event at the end of the log.
func (log *Log) Append(ctx context.Context, raw []byte) error {
	return log.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{"data": raw},
	}).Err()
}

// Replay reads the whole log from the start, in batches, and calls handle for
// each stored event in order.
func (log *Log) Replay(ctx context.Context, handle func(raw []byte) error) error {
	start := "-"
	var lastID string
	for {
		entries, err := log.client.XRangeN(ctx, streamKey, start, "+", log.batchSize).Result()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.ID == lastID {
				continue // boundary entry from the previous batch, already handled
			}
			data, _ := entry.Values["data"].(string)
			if err := handle([]byte(data)); err != nil {
				return err
			}
			lastID = entry.ID
		}
		if int64(len(entries)) < log.batchSize {
			return nil // last (partial) batch
		}
		start = lastID // inclusive; the duplicate boundary entry is skipped above
	}
}
