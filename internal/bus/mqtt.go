// Package bus is a thin wrapper over an MQTT connection. It is the live path
// events travel: the gateway publishes, the consumers subscribe.
package bus

import (
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// QoS 1 is "at least once": the broker redelivers until acknowledged, so a
// reconnecting client never silently loses messages. Duplicates are harmless
// because everything downstream is idempotent.
const QoS = 1

// Handler receives one delivered message.
type Handler func(topic string, payload []byte)

// Bus is one MQTT client connection.
type Bus struct {
	client mqtt.Client
}

// Connect opens one MQTT connection. clientID must be stable per role so the
// broker can keep a persistent session and replay missed messages on reconnect.
func Connect(broker, clientID string) (*Bus, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetCleanSession(false). // persistent session: broker queues for us while disconnected
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetResumeSubs(true)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(10 * time.Second) {
		return nil, fmt.Errorf("mqtt connect to %s timed out", broker)
	}
	if err := token.Error(); err != nil {
		return nil, err
	}
	return &Bus{client: client}, nil
}

// Publish sends one event. It does not wait for the broker ack: paho handles
// QoS1 retransmission in the background, and blocking per message would throttle
// the ingest accept path. When paho's in-flight window fills under load, Publish
// blocks, which is the backpressure we want (delay, never drop).
func (bus *Bus) Publish(topic string, payload []byte) {
	bus.client.Publish(topic, QoS, false, payload)
}

// Subscribe registers a handler for a topic filter.
func (bus *Bus) Subscribe(topic string, handle Handler) error {
	token := bus.client.Subscribe(topic, QoS, func(_ mqtt.Client, message mqtt.Message) {
		handle(message.Topic(), message.Payload())
	})
	token.Wait()
	return token.Error()
}

func (bus *Bus) Close() {
	bus.client.Disconnect(250)
}
