package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func TestProducerPublishesVersionedEventEnvelope(t *testing.T) {
	writer := &fakeWriter{}
	producer := NewProducer(writer)
	event := domain.Event{EventID: uuid.New(), Type: domain.EventTypeClick, CampaignID: uuid.New(), UserID: "user-1"}

	if err := producer.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("published %d messages, want 1", len(writer.messages))
	}
	var envelope domain.EventEnvelope
	if err := json.Unmarshal(writer.messages[0].Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.SchemaVersion != 1 || envelope.Event.EventID != event.EventID {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
	if string(writer.messages[0].Key) != event.EventID.String() || writer.messages[0].Topic != "" {
		t.Fatalf("unexpected message routing: %+v", writer.messages[0])
	}
}

func TestProducerPropagatesWriterFailure(t *testing.T) {
	wantErr := errors.New("broker unavailable")
	producer := NewProducer(&fakeWriter{err: wantErr})
	if err := producer.Publish(context.Background(), domain.Event{EventID: uuid.New()}); !errors.Is(err, wantErr) {
		t.Fatalf("error=%v, want %v", err, wantErr)
	}
}

type fakeWriter struct {
	messages []kafka.Message
	err      error
}

func (f *fakeWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	if f.err != nil {
		return f.err
	}
	f.messages = append(f.messages, messages...)
	return nil
}

func (f *fakeWriter) Close() error { return nil }
