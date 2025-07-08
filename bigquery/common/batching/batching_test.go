// Copyright (c) 2025 Skyflow, Inc.

package batching_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/skyflowapi/skyflow-integrations/bigquery/common/batching"
	. "github.com/skyflowapi/skyflow-integrations/bigquery/common/test"
)

type MockInput[K interface{}, V interface{}] struct {
	Key   K
	Value V
}

type KeyedBatch[K interface{}, V interface{}] struct {
	Key   K
	Batch batching.Batch[V]
}

var _ = Describe("Batches", func() {
	Context("when the batch capacity is 1", func() {
		It("should return a batch with the given capacity", func() {
			batch := batching.NewBatch[int](1)
			Expect(batch.Capacity()).To(Equal(1))
		})
	})

	Context("when the batch capacity is greater than 1", func() {
		It("should return a batch with the given capacity", func() {
			batch := batching.NewBatch[int](2)
			Expect(batch.Capacity()).To(Equal(2))
		})
	})

	Context("when the batch is created from indices and values", func() {
		Context("when the lengths and capacities are the same", func() {
			It("should return a batch with the given capacity", func() {
				batch, err := batching.NewBatchFrom([]int{0, 1}, []int{2, 4})
				Expect(err).To(BeNil())
				Expect(batch.Capacity()).To(Equal(2))
				Expect(batch.Size()).To(Equal(2))
			})
		})

		Context("when the lengths are different", func() {
			It("should return an error", func() {
				batch, err := batching.NewBatchFrom([]int{0, 1}, []int{2})
				Expect(batch).To(BeNil())
				Expect(err).To(MatchError("the number of indices and values must be the same: 2 != 1"))
			})
		})

		Context("when the capacities are different", func() {
			It("should return an error", func() {
				indices := make([]int, 2, 3)
				values := make([]int, 2, 4)
				batch, err := batching.NewBatchFrom(indices, values)
				Expect(batch).To(BeNil())
				Expect(err).To(MatchError("the capacities of the indices and values must be the same: 3 != 4"))
			})
		})
	})

	Context("when the batch is appended to", func() {
		It("should append the index and value to the batch", func() {
			batch := NewBatchWithCapacityFrom([]int{0, 1}, []int{2, 4}, 3)
			Expect(batch.Append(2, 6)).To(BeNil())
			Expect(batch.Indices).To(Equal([]int{0, 1, 2}))
			Expect(batch.Values).To(Equal([]int{2, 4, 6}))
			Expect(batch.Size()).To(Equal(3))
			Expect(batch.Capacity()).To(Equal(3))
		})

		Context("when the batch is full", func() {
			It("should return an error", func() {
				batch := NewBatchWithCapacityFrom([]int{0, 1}, []int{2, 4}, 2)
				err := batch.Append(2, 6)
				Expect(err).To(MatchError("the batch is full"))
			})
		})
	})

	Context("when the batch is reset", func() {
		It("should empty the values and indices", func() {
			batch, err := batching.NewBatchFrom([]int{0, 1}, []int{2, 4})
			Expect(err).To(BeNil())
			batch.Reset()
			Expect(batch.Indices).To(BeEmpty())
			Expect(batch.Values).To(BeEmpty())
			Expect(batch.Capacity()).To(Equal(2))
			Expect(cap(batch.Indices)).To(Equal(2))
			Expect(cap(batch.Values)).To(Equal(2))
			Expect(batch.Size()).To(Equal(0))
		})
	})

	Context("when the batch is full", func() {
		It("should return true for IsFull", func() {
			batch := NewBatchWithCapacityFrom([]int{0, 1}, []int{2, 4}, 2)
			Expect(batch.IsFull()).To(BeTrue())
		})

		It("should return false for IsEmpty", func() {
			batch := NewBatchWithCapacityFrom([]int{0, 1}, []int{2, 4}, 2)
			Expect(batch.IsEmpty()).To(BeFalse())
		})
	})

	Context("when the batch is empty", func() {
		It("should return true for IsEmpty", func() {
			batch := NewBatchWithCapacityFrom([]int{}, []int{}, 2)
			Expect(batch.IsEmpty()).To(BeTrue())
		})

		It("should return false for IsFull", func() {
			batch := NewBatchWithCapacityFrom([]int{}, []int{}, 2)
			Expect(batch.IsFull()).To(BeFalse())
		})
	})

	Context("when the batch is not full nor empty", func() {
		It("should return false for IsFull", func() {
			batch := NewBatchWithCapacityFrom([]int{0, 1}, []int{2, 4}, 3)
			Expect(batch.IsFull()).To(BeFalse())
		})

		It("should return false for IsEmpty", func() {
			batch := NewBatchWithCapacityFrom([]int{0, 1}, []int{2, 4}, 3)
			Expect(batch.IsEmpty()).To(BeFalse())
		})
	})
})

var _ = Describe("Batching", func() {
	Context("when the batch capacity is 1", func() {
		It("should submit each item in the input as a separate batch", func() {
			submittedBatches := []batching.Batch[int]{}
			submittedKeys := []int{}
			submitter := &MockBatchSubmitter[int, int]{
				DoSubmit: func(key int, batch batching.Batch[int]) error {
					submittedBatches = append(submittedBatches, batch)
					submittedKeys = append(submittedKeys, key)
					return nil
				},
			}
			keyGetter := &MockBatchKeyGetter[int, int]{
				DoGetBatchKey: func(input int) (int, error) {
					return input * 4, nil
				},
			}
			valueGetter := &MockBatchValueGetter[int, int]{
				DoGetBatchValue: func(input int) (int, error) {
					return input * 2, nil
				},
			}
			batcher := batching.NewBatcher(submitter, keyGetter, valueGetter, 1)

			Expect(batcher.Batch([]int{1, 2, 3})).To(BeNil())

			Expect(submittedBatches).To(HaveLen(3))
			Expect(submittedKeys).To(HaveLen(3))
			Expect(submittedKeys).To(Equal([]int{4, 8, 12}))
			Expect(submittedBatches).To(Equal([]batching.Batch[int]{
				*ExpectNilError(func() (*batching.Batch[int], error) { return batching.NewBatchFrom([]int{0}, []int{2}) }),
				*ExpectNilError(func() (*batching.Batch[int], error) { return batching.NewBatchFrom([]int{1}, []int{4}) }),
				*ExpectNilError(func() (*batching.Batch[int], error) { return batching.NewBatchFrom([]int{2}, []int{6}) }),
			}))
		})
	})

	Context("when the batch capacity is greater than 1", func() {
		It("should submit the batches grouped by key", func() {
			nSubmit := 0
			remainingKeyedBatches := []KeyedBatch[int, int]{}
			submitter := &MockBatchSubmitter[int, int]{
				DoSubmit: func(key int, batch batching.Batch[int]) error {
					switch nSubmit {
					case 0:
						Expect(key).To(Equal(1))
						Expect(batch.Indices).To(Equal([]int{0, 2}))
						Expect(batch.Values).To(Equal([]int{0, 6}))
					case 1:
						Expect(key).To(Equal(0))
						Expect(batch.Indices).To(Equal([]int{1, 4}))
						Expect(batch.Values).To(Equal([]int{3, 12}))
					default:
						// The above batches are sent immediately as they meet the batch capacity
						// These remaining batches are submitted when the input is exhausted, and therefore their order is not defined
						remainingKeyedBatches = append(remainingKeyedBatches, KeyedBatch[int, int]{Key: key, Batch: batch})
					}
					nSubmit++
					return nil
				},
			}
			keyGetter := &MockBatchKeyGetter[int, int]{
				DoGetBatchKey: func(input int) (int, error) {
					return map[int]int{
						0: 1,
						1: 0,
						2: 1,
						3: 2,
						4: 0,
						5: 1,
					}[input], nil
				},
			}
			valueGetter := &MockBatchValueGetter[int, int]{
				DoGetBatchValue: func(input int) (int, error) {
					return input * 3, nil
				},
			}
			batcher := batching.NewBatcher(submitter, keyGetter, valueGetter, 2)
			Expect(batcher.Batch([]int{0, 1, 2, 3, 4, 5})).To(BeNil())

			expected := []KeyedBatch[int, int]{
				{Key: 1, Batch: *NewBatchWithCapacityFrom([]int{5}, []int{15}, 2)},
				{Key: 2, Batch: *NewBatchWithCapacityFrom([]int{3}, []int{9}, 2)},
			}

			Expect(remainingKeyedBatches).To(ConsistOf(expected))
		})
	})

	Context("when the key getter returns an error", func() {
		It("should return an error", func() {
			submitter := &MockBatchSubmitter[int, int]{
				DoSubmit: func(key int, batch batching.Batch[int]) error {
					return nil
				},
			}
			keyGetter := &MockBatchKeyGetter[int, int]{
				DoGetBatchKey: func(input int) (int, error) {
					return 0, errors.New("test-error")
				},
			}
			valueGetter := &MockBatchValueGetter[int, int]{
				DoGetBatchValue: func(input int) (int, error) {
					return 0, nil
				},
			}
			batcher := batching.NewBatcher(submitter, keyGetter, valueGetter, 1)
			err := batcher.Batch([]int{0, 1, 2, 3, 4, 5})
			Expect(err).To(MatchError("error getting batch key: test-error"))
		})
	})

	Context("when the value getter returns an error", func() {
		It("should return an error", func() {
			submitter := &MockBatchSubmitter[int, int]{
				DoSubmit: func(key int, batch batching.Batch[int]) error {
					return nil
				},
			}
			keyGetter := &MockBatchKeyGetter[int, int]{
				DoGetBatchKey: func(input int) (int, error) {
					return 0, nil
				},
			}
			valueGetter := &MockBatchValueGetter[int, int]{
				DoGetBatchValue: func(input int) (int, error) {
					return 0, errors.New("test-error")
				},
			}
			batcher := batching.NewBatcher(submitter, keyGetter, valueGetter, 1)
			err := batcher.Batch([]int{0, 1, 2, 3, 4, 5})
			Expect(err).To(MatchError("error getting batch value: test-error"))
		})
	})

	Context("when the submitter returns an error", func() {
		Context("when the batch capacity is reached", func() {
			It("should return an error", func() {
				submitter := &MockBatchSubmitter[int, int]{
					DoSubmit: func(key int, batch batching.Batch[int]) error {
						return errors.New("test-error")
					},
				}
				keyGetter := &MockBatchKeyGetter[int, int]{
					DoGetBatchKey: func(input int) (int, error) {
						return 0, nil
					},
				}
				valueGetter := &MockBatchValueGetter[int, int]{
					DoGetBatchValue: func(input int) (int, error) {
						return 0, nil
					},
				}
				batcher := batching.NewBatcher(submitter, keyGetter, valueGetter, 1)
				err := batcher.Batch([]int{0, 1, 2, 3, 4, 5})
				Expect(err).To(MatchError("error submitting batch: test-error"))
			})
		})

		Context("when the input is exhausted before the batch capacity is reached", func() {
			It("should return an error", func() {
				submitter := &MockBatchSubmitter[int, int]{
					DoSubmit: func(key int, batch batching.Batch[int]) error {
						return errors.New("test-error")
					},
				}
				keyGetter := &MockBatchKeyGetter[int, int]{
					DoGetBatchKey: func(input int) (int, error) {
						return 0, nil
					},
				}
				valueGetter := &MockBatchValueGetter[int, int]{
					DoGetBatchValue: func(input int) (int, error) {
						return 0, nil
					},
				}
				batcher := batching.NewBatcher(submitter, keyGetter, valueGetter, 10)
				err := batcher.Batch([]int{0, 1, 2, 3, 4, 5})
				Expect(err).To(MatchError("error submitting batch: test-error"))
			})
		})
	})
})
