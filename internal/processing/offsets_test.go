package processing

import (
	"testing"
)

func TestOffsetCoordinatorCommitsOnlyContiguousCompletions(t *testing.T) {
	coordinator := NewOffsetCoordinator()
	coordinator.Observe(0, 10)
	coordinator.Observe(0, 11)
	coordinator.Observe(0, 12)

	if _, ok := coordinator.Complete(0, 11); ok {
		t.Fatal("completed offset 11 before offset 10")
	}
	if offset, ok := coordinator.Complete(0, 10); !ok || offset != 11 {
		t.Fatalf("first contiguous offset=(%d,%v), want (11,true)", offset, ok)
	}
	if offset, ok := coordinator.Complete(0, 12); !ok || offset != 12 {
		t.Fatalf("second contiguous offset=(%d,%v), want (12,true)", offset, ok)
	}
}

func TestOffsetCoordinatorTracksPartitionsIndependently(t *testing.T) {
	coordinator := NewOffsetCoordinator()
	coordinator.Observe(0, 5)
	coordinator.Observe(1, 8)
	if offset, ok := coordinator.Complete(1, 8); !ok || offset != 8 {
		t.Fatalf("partition 1 completion=(%d,%v)", offset, ok)
	}
	if _, ok := coordinator.Complete(0, 6); ok {
		t.Fatal("unobserved partition offset was committed")
	}
}
