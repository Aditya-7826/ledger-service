package outbox

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Aditya-7826/ledger-service/internal/events"
)

// KafkaPublisher publishes outbox messages with franz-go's idempotent
// producer (acks=all).
type KafkaPublisher struct {
	client *kgo.Client
}

// NewKafkaPublisher connects to the brokers. Idempotent production and
// acks from all in-sync replicas are franz-go defaults; they are set
// explicitly here because the relay's correctness depends on them.
func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{client: client}, nil
}

// Publish sends the batch and waits for every acknowledgement. The record
// key is the aggregate ID, so all events of one account (or one transfer)
// go to the same partition, in order.
func (p *KafkaPublisher) Publish(ctx context.Context, msgs []Message) error {
	records := make([]*kgo.Record, 0, len(msgs))
	for _, m := range msgs {
		topic, ok := events.Topic(m.EventType)
		if !ok {
			return fmt.Errorf("outbox row %d has unknown event type %q", m.ID, m.EventType)
		}
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   []byte(m.AggregateID.String()),
			Value: m.Payload,
			Headers: []kgo.RecordHeader{
				{Key: "event_id", Value: []byte(m.EventID.String())},
				{Key: "event_type", Value: []byte(m.EventType)},
			},
		})
	}
	return p.client.ProduceSync(ctx, records...).FirstErr()
}

// Close flushes and closes the client.
func (p *KafkaPublisher) Close() { p.client.Close() }
