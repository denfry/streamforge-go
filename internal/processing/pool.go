package processing

import (
	"context"
	"sync"

	"github.com/denfry/streamforge-go/internal/domain"
	"github.com/segmentio/kafka-go"
)

type Job struct {
	Message   kafka.Message
	Event     domain.Event
	DecodeErr error
}

type Handler func(context.Context, Job) error

type Result struct {
	Job Job
	Err error
}

type Pool struct {
	workers int
	handler Handler
	jobs    chan Job
	results chan Result
	done    chan struct{}
	close   sync.Once
}

func NewPool(workerCount, queueCapacity int, handler Handler) *Pool {
	if workerCount < 1 {
		workerCount = 1
	}
	if queueCapacity < 1 {
		queueCapacity = workerCount
	}
	return &Pool{
		workers: workerCount,
		handler: handler,
		jobs:    make(chan Job, queueCapacity),
		results: make(chan Result, queueCapacity),
		done:    make(chan struct{}),
	}
}

func (p *Pool) Run(ctx context.Context) {
	var group sync.WaitGroup
	group.Add(p.workers)
	for range p.workers {
		go func() {
			defer group.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-p.jobs:
					if !ok {
						return
					}
					result := Result{Job: job, Err: p.handler(ctx, job)}
					select {
					case p.results <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	group.Wait()
	close(p.results)
	close(p.done)
}

func (p *Pool) Submit(ctx context.Context, job Job) error {
	select {
	case <-p.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case <-p.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	case p.jobs <- job:
		return nil
	}
}

func (p *Pool) Results() <-chan Result {
	return p.results
}

func (p *Pool) CloseInput() {
	p.close.Do(func() { close(p.jobs) })
}

func (p *Pool) Depth() int {
	return len(p.jobs)
}
