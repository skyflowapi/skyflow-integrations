package messaging_test

import (
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
	. "github.com/skyflowapi/skyflow-integrations/bigquery/common/test"
)

var _ = Describe("Status code checks", func() {
	handler := func(check func(statusCode int) bool, statusCode int, expected bool) {
		Expect(check(statusCode)).To(Equal(expected))
	}

	args := make([]any, 0)
	args = append(args, handler)

	for i := range 500 {
		statusCode := 100 + i
		args = append(
			args,
			Entry(fmt.Sprintf("informational check when the status code is %d", statusCode), messaging.IsInformationalStatusCode, statusCode, 100 <= statusCode && statusCode < 200),
			Entry(fmt.Sprintf("successful check when the status code is %d", statusCode), messaging.IsSuccessfulStatusCode, statusCode, 200 <= statusCode && statusCode < 300),
			Entry(fmt.Sprintf("redirection check when the status code is %d", statusCode), messaging.IsRedirectionStatusCode, statusCode, 300 <= statusCode && statusCode < 400),
			Entry(fmt.Sprintf("client error check when the status code is %d", statusCode), messaging.IsClientErrorStatusCode, statusCode, 400 <= statusCode && statusCode < 500),
			Entry(fmt.Sprintf("server error check when the status code is %d", statusCode), messaging.IsServerErrorStatusCode, statusCode, 500 <= statusCode && statusCode < 600),
		)
	}

	DescribeTable("checking the status code", args...)
})

var _ = Describe("Create default exponential backoff", func() {
	It("should return a default exponential backoff", func() {
		backoff := messaging.DefaultExponentialBackoff()
		Expect(backoff.MaxTries).To(Equal(uint(3)))
		Expect(backoff.InitialInterval).To(Equal(1 * time.Second))
		Expect(backoff.RandomizationFactor).To(Equal(0.30))
		Expect(backoff.Multiplier).To(Equal(float64(2)))
		Expect(backoff.MaxInterval).To(Equal(20 * time.Second))
	})
})

var _ = Describe("Send request", func() {
	Context("when a permanent error is encountered", func() {
		It("should return the error", func() {
			backoff := messaging.DefaultExponentialBackoff()
			flow := &MockRequestFlow{
				SendFunc: func() error {
					return errors.New("test error")
				},
				IsDoneFunc: func() bool {
					return false
				},
			}
			err := messaging.SendRequest(flow, backoff)
			Expect(err).To(MatchError("test error"))
		})
	})

	Context("when a recoverable error is encountered", func() {
		It("should retry the request", func() {
			maxTries := uint(3)
			backoff := TestExponentialBackoff()
			backoff.MaxTries = maxTries
			nSend := uint(0)
			flow := &MockRequestFlow{
				SendFunc: func() error {
					nSend++
					return nil
				},
				IsDoneFunc: func() bool {
					return false
				},
			}
			err := messaging.SendRequest(flow, backoff)
			Expect(err).To(BeNil())
			Expect(nSend).To(Equal(maxTries))
		})
	})

	Context("when the request succeeds", func() {
		It("should return nil and not retry", func() {
			maxTries := uint(10)
			backoff := TestExponentialBackoff()
			backoff.MaxTries = maxTries
			succeedAfter := uint(3)
			nSend := uint(0)
			flow := &MockRequestFlow{
				SendFunc: func() error {
					nSend++
					return nil
				},
				IsDoneFunc: func() bool {
					return nSend >= succeedAfter
				},
			}
			err := messaging.SendRequest(flow, backoff)
			Expect(err).To(BeNil())
			Expect(nSend).To(Equal(succeedAfter))
			Expect(nSend).To(BeNumerically("<", maxTries))
		})
	})
})
