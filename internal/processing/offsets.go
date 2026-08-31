package processing

import "sync"

type OffsetCoordinator struct {
	mu         sync.Mutex
	partitions map[int]*partitionOffsets
}

type partitionOffsets struct {
	next      int64
	observed  bool
	completed map[int64]struct{}
}

func NewOffsetCoordinator() *OffsetCoordinator {
	return &OffsetCoordinator{partitions: make(map[int]*partitionOffsets)}
}

func (c *OffsetCoordinator) Observe(partition int, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.partition(partition)
	if !state.observed || offset < state.next {
		state.next = offset
		state.observed = true
	}
}

func (c *OffsetCoordinator) Complete(partition int, offset int64) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.partition(partition)
	if !state.observed || offset < state.next {
		return 0, false
	}
	if offset > state.next {
		state.completed[offset] = struct{}{}
		return 0, false
	}

	last := offset
	state.next++
	for {
		if _, ok := state.completed[state.next]; !ok {
			break
		}
		delete(state.completed, state.next)
		last = state.next
		state.next++
	}
	return last, true
}

func (c *OffsetCoordinator) Pending(partition int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.partition(partition).completed)
}

func (c *OffsetCoordinator) partition(partition int) *partitionOffsets {
	state := c.partitions[partition]
	if state == nil {
		state = &partitionOffsets{completed: make(map[int64]struct{})}
		c.partitions[partition] = state
	}
	return state
}
