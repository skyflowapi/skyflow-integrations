// Copyright (c) 2025 Skyflow, Inc.

package messaging_test

import (
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
)

var _ = Describe("Cloud trace context", func() {
	Context("when the header is not present", func() {
		It("should return an error", func() {
			header := http.Header{}
			context, err := messaging.GCPCloudTraceContextFromHeader(header)
			Expect(err).To(MatchError("header does not contain X-Cloud-Trace-Context"))
			Expect(context).To(BeNil())
		})
	})

	Context("when the header is present", func() {
		It("should return a context", func() {
			header := http.Header{}
			header.Set("X-Cloud-Trace-Context", "6C26BC42CF33A48D29746CAF746FAF7B/7A6A9D96523F891AD4A2B2DBFDDDBA2EA57C6EFAC606A698462945845934CB2F/o=1")
			context, err := messaging.GCPCloudTraceContextFromHeader(header)
			Expect(err).To(BeNil())
			Expect(context).ToNot(BeNil())
			Expect(context.TraceID("foobar")).To(Equal("projects/foobar/traces/6C26BC42CF33A48D29746CAF746FAF7B"))
		})
	})

	Context("when the project ID is empty", func() {
		It("should return an error", func() {
			header := http.Header{}
			header.Set("X-Cloud-Trace-Context", "foo")
			context, err := messaging.GCPCloudTraceContextFromHeader(header)
			Expect(err).To(BeNil())
			Expect(context).ToNot(BeNil())
			traceID, err := context.TraceID("")
			Expect(err).To(MatchError("projectID is required to derive the GCP cloud trace ID"))
			Expect(traceID).To(BeEmpty())
		})
	})
})
