package eventlog

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestReplayRoundTrip appends events to a fake Redis and checks that Replay
// returns them all, in order, across multiple batches.
func TestReplayRoundTrip(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	log := New(client)
	log.batchSize = 3 // force several batches
	ctx := context.Background()

	var want [][]byte
	for i := 0; i < 10; i++ {
		raw := []byte(fmt.Sprintf(`{"seq":%d}`, i))
		want = append(want, raw)
		if err := log.Append(ctx, raw); err != nil {
			t.Fatal(err)
		}
	}

	var got [][]byte
	err = log.Replay(ctx, func(raw []byte) error {
		got = append(got, append([]byte(nil), raw...))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != len(want) {
		t.Fatalf("replayed %d events, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("event %d: got %s, want %s", i, got[i], want[i])
		}
	}
}

func TestReplayEmpty(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	ctx := context.Background()

	calls := 0
	if err := New(client).Replay(ctx, func([]byte) error { calls++; return nil }); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("empty log replayed %d events", calls)
	}
}
