package batching

import (
	"fmt"
)

type Batch[V interface{}] struct {
	Indices []int
	Values  []V
	// The capacity of the batch; is static and should not change.
	capacity int
}

func NewBatch[V interface{}](capacity int) *Batch[V] {
	return &Batch[V]{
		Indices:  make([]int, 0, capacity),
		Values:   make([]V, 0, capacity),
		capacity: capacity,
	}
}

func NewBatchFrom[V interface{}](indices []int, values []V) (*Batch[V], error) {
	if len(indices) != len(values) {
		return nil, fmt.Errorf("the number of indices and values must be the same: %d != %d", len(indices), len(values))
	}
	if cap(indices) != cap(values) {
		return nil, fmt.Errorf("the capacities of the indices and values must be the same: %d != %d", cap(indices), cap(values))
	}
	capacity := cap(indices)
	batch := &Batch[V]{
		Indices:  indices,
		Values:   values,
		capacity: capacity,
	}
	return batch, nil
}

func (batch *Batch[V]) Append(index int, value V) error {
	if batch.IsFull() {
		return fmt.Errorf("the batch is full")
	}
	batch.Indices = append(batch.Indices, index)
	batch.Values = append(batch.Values, value)
	return nil
}

func (batch *Batch[V]) IsFull() bool {
	return batch.Size() == batch.capacity
}

func (batch *Batch[V]) IsEmpty() bool {
	return batch.Size() == 0
}

func (batch *Batch[V]) Reset() {
	batch.Indices = batch.Indices[:0]
	batch.Values = batch.Values[:0]
}

func (batch *Batch[V]) Size() int {
	return len(batch.Indices)
}

func (batch *Batch[V]) Capacity() int {
	return batch.capacity
}

type BatchSubmitter[K interface{}, V interface{}] interface {
	Submit(key K, batch Batch[V]) error
}

type BatchKeyGetter[I interface{}, K interface{}] interface {
	GetBatchKey(I) (K, error)
}

type BatchValueGetter[I interface{}, V interface{}] interface {
	GetBatchValue(I) (V, error)
}

type Batcher[I interface{}, K comparable, V interface{}] struct {
	submitter   BatchSubmitter[K, V]
	keyGetter   BatchKeyGetter[I, K]
	valueGetter BatchValueGetter[I, V]
	capacity    int
}

func (batcher *Batcher[I, K, V]) Batch(in []I) error {
	batchByKey := make(map[K]*Batch[V])
	for i := range in {
		key, err := batcher.keyGetter.GetBatchKey(in[i])
		if err != nil {
			return fmt.Errorf("error getting batch key: %w", err)
		}

		value, err := batcher.valueGetter.GetBatchValue(in[i])
		if err != nil {
			return fmt.Errorf("error getting batch value: %w", err)
		}

		batch, ok := batchByKey[key]
		if !ok {
			batch = NewBatch[V](batcher.capacity)
			batchByKey[key] = batch
		}

		err = batch.Append(i, value)
		if err != nil {
			return fmt.Errorf("error appending to batch: %w", err)
		}

		if batch.IsFull() {
			if err := batcher.submit(key, batch); err != nil {
				return fmt.Errorf("error submitting batch: %w", err)
			}
		}
	}

	for key, batch := range batchByKey {
		if err := batcher.submit(key, batch); err != nil {
			return fmt.Errorf("error submitting batch: %w", err)
		}
	}

	return nil
}

func (batcher *Batcher[I, K, V]) submit(key K, batch *Batch[V]) error {
	if batch.IsEmpty() {
		return nil
	}
	err := batcher.submitter.Submit(key, *batch)
	if err != nil {
		return err
	}
	batch.Reset()
	return nil
}

func NewBatcher[I interface{}, K comparable, V interface{}](
	submitter BatchSubmitter[K, V],
	keyGetter BatchKeyGetter[I, K],
	valueGetter BatchValueGetter[I, V],
	capacity int,
) *Batcher[I, K, V] {
	return &Batcher[I, K, V]{
		submitter:   submitter,
		keyGetter:   keyGetter,
		valueGetter: valueGetter,
		capacity:    capacity,
	}
}
