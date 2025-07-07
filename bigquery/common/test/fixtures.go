// Copyright (c) 2025 Skyflow, Inc.

package test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck
	. "github.com/onsi/gomega"    //nolint:staticcheck
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/batching"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/logging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
)

type MockReadCloser struct {
	io.ReadCloser
	NCloseCalls int
	NReadCalls  int
}

func (r *MockReadCloser) Close() error {
	r.NCloseCalls++
	return r.ReadCloser.Close()
}

func (r *MockReadCloser) Read(p []byte) (int, error) {
	r.NReadCalls++
	return r.ReadCloser.Read(p)
}

func MarshalAndStringify(v interface{}) string {
	GinkgoHelper()
	json, err := json.Marshal(v)
	Expect(err).To(BeNil())
	return string(json)
}

func ToPtr[T any](v T) *T {
	return &v
}

type MockHttpClient struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHttpClient) Do(req *http.Request) (*http.Response, error) {
	return m.DoFunc(req)
}

type MockRequestFlow struct {
	SendFunc   func() error
	IsDoneFunc func() bool
}

func (m *MockRequestFlow) Send() error {
	return m.SendFunc()
}

func (m *MockRequestFlow) IsDone() bool {
	return m.IsDoneFunc()
}

type MockLogger struct {
	Context []string
	Logs    []MockLog
}

type MockLog struct {
	Level    logging.Level
	Messages []string
}

func (m *MockLogger) WithContext(context ...string) logging.Logger {
	m.Context = append(m.Context, context...)
	return m
}

func (m *MockLogger) FormatAndAddContext(level logging.Level, messages ...string) string {
	return strings.Join(messages, " ")
}

func (m *MockLogger) SetLevel(level logging.Level) {}

func (m *MockLogger) Fatal(messages ...string) {
	m.addMessage(logging.FatalLevel, messages...)
}

func (m *MockLogger) FatalNoContextNoFormat(message string) {
	m.addMessage(logging.FatalLevel, message)
}

func (m *MockLogger) Error(messages ...string) {
	m.addMessage(logging.ErrorLevel, messages...)
}

func (m *MockLogger) ErrorNoContextNoFormat(message string) {
	m.addMessage(logging.ErrorLevel, message)
}

func (m *MockLogger) Warn(messages ...string) {
	m.addMessage(logging.WarnLevel, messages...)
}

func (m *MockLogger) WarnNoContextNoFormat(message string) {
	m.addMessage(logging.WarnLevel, message)
}

func (m *MockLogger) Info(messages ...string) {
	m.addMessage(logging.InfoLevel, messages...)
}

func (m *MockLogger) InfoNoContextNoFormat(message string) {
	m.addMessage(logging.InfoLevel, message)
}

func (m *MockLogger) addMessage(level logging.Level, messages ...string) {
	m.Logs = append(m.Logs, MockLog{Level: level, Messages: messages})
}

func ExpectNilError[T any](f func() (T, error)) T {
	value, err := f()
	Expect(err).To(BeNil())
	return value
}

// Resizes the indices and values to the given capacity before returning a new batch.
func NewBatchWithCapacityFrom[V interface{}](indices []int, values []V, capacity int) *batching.Batch[V] {
	newIndices := make([]int, len(indices), capacity)
	copy(newIndices, indices)
	newValues := make([]V, len(values), capacity)
	copy(newValues, values)
	batch, err := batching.NewBatchFrom(newIndices, newValues)
	Expect(err).To(BeNil())
	return batch
}

type MockBatchSubmitter[K interface{}, V interface{}] struct {
	DoSubmit func(key K, batch batching.Batch[V]) error
}

func (submitter *MockBatchSubmitter[K, V]) Submit(key K, batch batching.Batch[V]) error {
	return submitter.DoSubmit(key, batch)
}

type MockBatchKeyGetter[I interface{}, K interface{}] struct {
	DoGetBatchKey func(I) (K, error)
}

func (getter *MockBatchKeyGetter[I, K]) GetBatchKey(input I) (K, error) {
	return getter.DoGetBatchKey(input)
}

type MockBatchValueGetter[I interface{}, V interface{}] struct {
	DoGetBatchValue func(I) (V, error)
}

func (getter *MockBatchValueGetter[I, V]) GetBatchValue(input I) (V, error) {
	return getter.DoGetBatchValue(input)
}

func TestExponentialBackoff() *messaging.ExponentialBackoff {
	backoff := messaging.DefaultExponentialBackoff()
	backoff.MaxInterval = 0 // no delay between retries by default
	return backoff
}
