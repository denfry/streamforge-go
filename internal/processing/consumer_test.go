package processing

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func TestConsumerCommitsAfterSuccessfulProcessing(t *testing.T) {
	message := eventMessage(4)
	reader := &fakeReader{messages: []kafka.Message{message}}
	analyticsStore := &fakeAnalytics{}
	consumer := NewConsumer(ConsumerDependencies{
		Reader:         reader,
		Analytics:      analyticsStore,
		DLQ:            &fakeDLQ{},
		WorkerCount:    1,
		QueueCapacity:  1,
		Retry:          RetryPolicy{MaxAttempts: 1},
		ProcessTimeout: time.Second,
		DLQTimeout:     time.Second,
	})

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(reader.commits) != 1 || reader.commits[0].Offset != 4 {
		t.Fatalf("commits=%+v, want offset 4", reader.commits)
	}
	if analyticsStore.inserts != 1 {
		t.Fatalf("inserts=%d, want 1", analyticsStore.inserts)
	}
}

func TestConsumerPublishesDLQAfterFinalRetry(t *testing.T) {
	reader := &fakeReader{messages: []kafka.Message{eventMessage(8)}}
	dlq := &fakeDLQ{}
	consumer := NewConsumer(ConsumerDependencies{
		Reader:         reader,
		Analytics:      &fakeAnalytics{err: errors.New("clickhouse unavailable")},
		DLQ:            dlq,
		WorkerCount:    1,
		QueueCapacity:  1,
		Retry:          RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, Classify: func(error) bool { return true }},
		ProcessTimeout: time.Second,
		DLQTimeout:     time.Second,
	})

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dlq.messages) != 1 || len(reader.commits) != 1 {
		t.Fatalf("dlq=%d commits=%d, want one each", len(dlq.messages), len(reader.commits))
	}
	if dlq.messages[0].Attempts != 2 {
		t.Fatalf("attempts=%d, want 2", dlq.messages[0].Attempts)
	}
}

func eventMessage(offset int64) kafka.Message {
	event := domain.Event{EventID: uuid.New(), Type: domain.EventTypeImpression, CampaignID: uuid.New(), UserID: "user-1", OccurredAt: time.Now().UTC()}
	payload, _ := json.Marshal(domain.EventEnvelope{SchemaVersion: 1, Event: event})
	return kafka.Message{Topic: "streamforge.events", Partition: 0, Offset: offset, Value: payload}
}

type fakeReader struct {
	messages []kafka.Message
	index    int
	commits  []kafka.Message
}

func (f *fakeReader) FetchMessage(context.Context) (kafka.Message, error) {
	if f.index >= len(f.messages) {
		return kafka.Message{}, io.EOF
	}
	message := f.messages[f.index]
	f.index++
	return message, nil
}

func (f *fakeReader) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	f.commits = append(f.commits, messages...)
	return nil
}

func (f *fakeReader) Close() error { return nil }

type fakeAnalytics struct {
	inserts int
	err     error
}

func (f *fakeAnalytics) InsertEvent(context.Context, domain.Event, time.Time, int) error {
	f.inserts++
	return f.err
}
func (f *fakeAnalytics) CampaignStats(context.Context, uuid.UUID, time.Time, time.Time) (domain.CampaignStats, error) {
	return domain.CampaignStats{}, nil
}
func (f *fakeAnalytics) Ping(context.Context) error { return nil }
func (f *fakeAnalytics) Close() error               { return nil }

type fakeDLQ struct {
	messages []domain.FailureMetadata
}

func (f *fakeDLQ) Publish(_ context.Context, _ domain.Event, _ kafka.Message, failure domain.FailureMetadata) error {
	f.messages = append(f.messages, failure)
	return nil
}
