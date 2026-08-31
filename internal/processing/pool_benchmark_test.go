package processing

import (
	"context"
	"testing"
)

func BenchmarkPoolSubmit(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := NewPool(4, 64, func(context.Context, Job) error { return nil })
	go pool.Run(ctx)
	resultsDone := make(chan struct{})
	go func() {
		for range pool.Results() {
		}
		close(resultsDone)
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := pool.Submit(ctx, Job{}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	pool.CloseInput()
	<-resultsDone
}
