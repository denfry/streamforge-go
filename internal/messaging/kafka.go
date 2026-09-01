package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Writer interface {
	WriteMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

type Producer struct {
	writer Writer
}

func NewProducer(writer Writer) *Producer {
	return &Producer{writer: writer}
}

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxAttempts:  3,
		RequiredAcks: int(kafka.RequireAll),
	})
}

func (p *Producer) Publish(ctx context.Context, event domain.Event) error {
	payload, err := json.Marshal(domain.EventEnvelope{SchemaVersion: 1, Event: event})
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.EventID.String()),
		Value: payload,
		Time:  time.Now().UTC(),
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

type DLQPublisher struct {
	writer Writer
}

func NewDLQPublisher(writer Writer) *DLQPublisher {
	return &DLQPublisher{writer: writer}
}

func (p *DLQPublisher) Publish(ctx context.Context, event domain.Event, source kafka.Message, failure domain.FailureMetadata) error {
	payload, err := json.Marshal(domain.EventEnvelope{SchemaVersion: 1, Event: event, Failure: &failure})
	if err != nil {
		return fmt.Errorf("encode dead-letter event: %w", err)
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.EventID.String()),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "source-topic", Value: []byte(source.Topic)},
			{Key: "source-partition", Value: []byte(fmt.Sprintf("%d", source.Partition))},
			{Key: "source-offset", Value: []byte(fmt.Sprintf("%d", source.Offset))},
		},
		Time: time.Now().UTC(),
	})
}

func EventIDFromMessage(message kafka.Message) (uuid.UUID, error) {
	var envelope domain.EventEnvelope
	if err := json.Unmarshal(message.Value, &envelope); err != nil {
		return uuid.Nil, fmt.Errorf("decode event envelope: %w", err)
	}
	if envelope.SchemaVersion != 1 {
		return uuid.Nil, fmt.Errorf("unsupported event schema version %d", envelope.SchemaVersion)
	}
	return envelope.Event.EventID, nil
}
