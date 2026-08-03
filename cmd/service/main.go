// Command service is the streaming backend: it ingests events over HTTP,
// carries them over MQTT, persists them to a Redis-backed log, and serves the
// real-time health, occupancy, and alarm queries.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/iskynsn/teton-streaming-backend/internal/agg"
	"github.com/iskynsn/teton-streaming-backend/internal/api"
	"github.com/iskynsn/teton-streaming-backend/internal/bus"
	"github.com/iskynsn/teton-streaming-backend/internal/domain"
	"github.com/iskynsn/teton-streaming-backend/internal/eventlog"
	"github.com/iskynsn/teton-streaming-backend/internal/ingest"
	"github.com/iskynsn/teton-streaming-backend/internal/obs"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	port := env("PORT", "8080")
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	mqttBroker := env("MQTT_BROKER", "tcp://localhost:1883")

	metrics := obs.New(logger)
	store := agg.NewStore()
	broker := api.NewBroker()
	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return err
	}
	log := eventlog.New(redisClient)

	// Rebuild every view by replaying the log before we start serving.
	var replayed int64
	if err := log.Replay(ctx, func(raw []byte) error {
		apply(store, raw, nil) // nil broker: no live subscribers during replay
		replayed++
		return nil
	}); err != nil {
		return err
	}
	metrics.Replayed(replayed)
	logger.Info("replay complete", "events", replayed)

	// One connection to publish, plus separate connections for the bulk and
	// fall consumers so a heartbeat flood can't delay fall delivery.
	publisher, err := bus.Connect(mqttBroker, "gateway-publisher")
	if err != nil {
		return err
	}
	bulk, err := bus.Connect(mqttBroker, "consumer-bulk")
	if err != nil {
		return err
	}
	falls, err := bus.Connect(mqttBroker, "consumer-falls")
	if err != nil {
		return err
	}

	// Bulk lane: the types that feed health and occupancy. Persist, then apply.
	bulkHandler := func(_ string, raw []byte) {
		_ = log.Append(ctx, raw)
		apply(store, raw, nil)
	}
	for _, eventType := range []domain.EventType{domain.Heartbeat, domain.Presence} {
		if err := bulk.Subscribe("events/"+string(eventType)+"/#", bulkHandler); err != nil {
			return err
		}
	}

	// Fast lane: falls. Persist, apply, and push new alarms to the live feed.
	if err := falls.Subscribe("events/fall_warn/#", func(_ string, raw []byte) {
		_ = log.Append(ctx, raw)
		if alarm, isNew := apply(store, raw, broker); isNew {
			metrics.Alarm()
			logger.Info("alarm", "event_id", alarm.EventID, "room", alarm.RoomID)
		}
	}); err != nil {
		return err
	}

	gateway := ingest.NewGateway(publisher, metrics)
	server := &http.Server{
		Addr:    ":" + port,
		Handler: api.NewServer(store, broker, gateway, metrics).Routes(),
	}

	go func() {
		logger.Info("listening", "port", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped", "err", err)
		}
	}()

	// Wait for a signal, then shut down cleanly.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	publisher.Close()
	bulk.Close()
	falls.Close()
	return nil
}

// apply parses one raw event and routes it to the store. When broker is set and
// the event is a new fall, the resulting alarm is pushed to the live feed.
func apply(store *agg.Store, raw []byte, broker *api.Broker) (agg.Alarm, bool) {
	var event domain.Event
	if err := json.Unmarshal(raw, &event); err != nil {
		return agg.Alarm{}, false
	}
	eventTime, err := event.Time()
	if err != nil {
		return agg.Alarm{}, false
	}
	alarm, isNew := store.Apply(&event, eventTime)
	if isNew && broker != nil {
		broker.Publish(alarm)
	}
	return alarm, isNew
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
