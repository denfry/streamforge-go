package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/denfry/streamforge-go/internal/analytics"
	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/denfry/streamforge-go/internal/observability"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"io"
	"time"
)

type MessageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

type DeadLetterPublisher interface {
	Publish(context.Context, domain.Event, kafka.Message, domain.FailureMetadata) error
}
type Counter interface {
	IncrementEvent(context.Context, uuid.UUID, domain.EventType, time.Duration) error
}

type ConsumerDependencies struct {
	Reader         MessageReader
	Analytics      analytics.Store
	DLQ            DeadLetterPublisher
	Counter        Counter
	CounterTTL     time.Duration
	Metrics        *observability.Metrics
	WorkerCount    int
	QueueCapacity  int
	Retry          RetryPolicy
	ProcessTimeout time.Duration
	DLQTimeout     time.Duration
}

type Consumer struct {
	reader         MessageReader
	analytics      analytics.Store
	dlq            DeadLetterPublisher
	counter        Counter
	counterTTL     time.Duration
	metrics        *observability.Metrics
	workerCount    int
	queueCapacity  int
	retry          RetryPolicy
	processTimeout time.Duration
	dlqTimeout     time.Duration
	coordinator    *OffsetCoordinator
}

func NewConsumer(deps ConsumerDependencies) *Consumer {
	if deps.WorkerCount < 1 {
		deps.WorkerCount = 1
	}
	if deps.QueueCapacity < deps.WorkerCount {
		deps.QueueCapacity = deps.WorkerCount
	}
	if deps.ProcessTimeout <= 0 {
		deps.ProcessTimeout = 5 * time.Second
	}
	if deps.DLQTimeout <= 0 {
		deps.DLQTimeout = 2 * time.Second
	}
	if deps.CounterTTL <= 0 {
		deps.CounterTTL = 5 * time.Minute
	}
	return &Consumer{
		reader:         deps.Reader,
		analytics:      deps.Analytics,
		dlq:            deps.DLQ,
		counter:        deps.Counter,
		counterTTL:     deps.CounterTTL,
		metrics:        deps.Metrics,
		workerCount:    deps.WorkerCount,
		queueCapacity:  deps.QueueCapacity,
		retry:          deps.Retry,
		processTimeout: deps.ProcessTimeout,
		dlqTimeout:     deps.DLQTimeout,
		coordinator:    NewOffsetCoordinator(),
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.reader == nil || c.analytics == nil {
		return errors.New("consumer requires reader and analytics store")
	}
	poolCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	pool := NewPool(c.workerCount, c.queueCapacity, c.handle)
	go pool.Run(poolCtx)

	fetchErrors := make(chan error, 1)
	go func() {
		fetchErrors <- c.fetch(poolCtx, pool)
		pool.CloseInput()
	}()

	var firstErr error
	for result := range pool.Results() {
		if result.Err != nil {
			if c.metrics != nil {
				c.metrics.RecordProcessing("failed")
			}
			if firstErr == nil {
				firstErr = result.Err
			}
			continue
		}
		if offset, ok := c.coordinator.Complete(result.Job.Message.Partition, result.Job.Message.Offset); ok {
			commit := result.Job.Message
			commit.Offset = offset
			if err := c.reader.CommitMessages(ctx, commit); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("commit Kafka offset: %w", err)
			}
		}
	}
	if err := <-fetchErrors; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (c *Consumer) fetch(ctx context.Context, pool *Pool) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		job := Job{Message: message}
		var envelope domain.EventEnvelope
		if err := json.Unmarshal(message.Value, &envelope); err != nil {
			job.DecodeErr = fmt.Errorf("decode event envelope: %w", err)
		} else if envelope.SchemaVersion != 1 {
			job.DecodeErr = fmt.Errorf("unsupported event schema version %d", envelope.SchemaVersion)
		} else {
			job.Event = envelope.Event
		}
		c.coordinator.Observe(message.Partition, message.Offset)
		if err := pool.Submit(ctx, job); err != nil {
			return err
		}
		if c.metrics != nil {
			c.metrics.SetQueueDepth(pool.Depth())
		}
	}
}

func (c *Consumer) handle(ctx context.Context, job Job) error {
	if job.DecodeErr != nil {
		return c.publishDLQ(job, 1, job.DecodeErr)
	}
	processCtx, cancel := context.WithTimeout(ctx, c.processTimeout)
	defer cancel()
	attempt := 0
	attempts, err := c.retry.Run(processCtx, func(operationCtx context.Context) error {
		attempt++
		if attempt > 1 && c.metrics != nil {
			c.metrics.RecordRetry()
		}
		return c.analytics.InsertEvent(operationCtx, job.Event, time.Now().UTC(), attempt)
	})
	if err == nil {
		if c.counter != nil {
			_ = c.counter.IncrementEvent(ctx, job.Event.CampaignID, job.Event.Type, c.counterTTL)
		}
		if c.metrics != nil {
			c.metrics.RecordProcessing("success")
		}
		return nil
	}
	return c.publishDLQ(job, attempts, err)
}

func (c *Consumer) publishDLQ(job Job, attempts int, processingErr error) error {
	if c.dlq == nil {
		return processingErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.dlqTimeout)
	defer cancel()
	failure := domain.FailureMetadata{
		SourceTopic:     job.Message.Topic,
		SourcePartition: job.Message.Partition,
		SourceOffset:    job.Message.Offset,
		ErrorClass:      errorClass(processingErr),
		Attempts:        attempts,
	}
	if err := c.dlq.Publish(ctx, job.Event, job.Message, failure); err != nil {
		return fmt.Errorf("publish dead-letter event: %w", err)
	}
	if c.metrics != nil {
		c.metrics.RecordDLQ()
		c.metrics.RecordProcessing("dlq")
	}
	return nil
}

func errorClass(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "processing_error"
	}
}
