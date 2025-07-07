// Copyright (c) 2025 Skyflow, Inc.

package messaging_test

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"

	"github.com/gin-gonic/gin"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
)

func CreateBigQueryRequestBody(calls interface{}, requestId interface{}) *bytes.Buffer {
	GinkgoHelper()
	body, err := json.Marshal(map[string]interface{}{
		"calls":     calls,
		"requestId": requestId,
	})
	Expect(err).NotTo(HaveOccurred())
	return bytes.NewBuffer(body)
}

func CreateBigQueryRequest(body *bytes.Buffer) (*httptest.ResponseRecorder, *gin.Context) {
	GinkgoHelper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("POST", "/", body)
	return recorder, ctx
}

var _ = Describe("Binding a BigQuery request", func() {
	Context("when the request is valid", func() {
		It("will successfully bind the request", func() {
			calls := [][]interface{}{{"1", "2", "3"}, {"4", "5", "6"}}
			requestId := "123"

			_, ctx := CreateBigQueryRequest(CreateBigQueryRequestBody(calls, requestId))
			req, err := messaging.BindBigQueryRequest(ctx)

			Expect(err).NotTo(HaveOccurred())
			Expect(req.Calls).To(Equal(calls))
			Expect(*req.RequestIdempotencyKey).To(Equal(requestId))
		})
	})

	Context("when the calls are not an array of arrays", func() {
		It("will return an error", func() {
			_, ctx := CreateBigQueryRequest(CreateBigQueryRequestBody("invalid", "123"))

			req, err := messaging.BindBigQueryRequest(ctx)

			Expect(err).To(HaveOccurred())
			Expect(req).To(BeNil())
		})
	})

	Context("when the calls are missing", func() {
		It("the array of calls in the result will be empty", func() {
			body, err := json.Marshal(map[string]interface{}{
				"requestId": "123",
			})
			Expect(err).NotTo(HaveOccurred())

			_, ctx := CreateBigQueryRequest(bytes.NewBuffer(body))
			req, err := messaging.BindBigQueryRequest(ctx)

			Expect(err).NotTo(HaveOccurred())
			Expect(req.Calls).To(BeEmpty())
		})
	})

	Context("when the requestId is not a string", func() {
		It("will return an error", func() {
			_, ctx := CreateBigQueryRequest(CreateBigQueryRequestBody([][]interface{}{{"1", "2", "3"}, {"4", "5", "6"}}, 123))

			req, err := messaging.BindBigQueryRequest(ctx)

			Expect(err).To(HaveOccurred())
			Expect(req).To(BeNil())
		})
	})

	Context("when the requestId is missing", func() {
		It("will return an error", func() {
			body, err := json.Marshal(map[string]interface{}{
				"calls": [][]interface{}{{"1", "2", "3"}, {"4", "5", "6"}},
			})
			Expect(err).NotTo(HaveOccurred())

			_, ctx := CreateBigQueryRequest(bytes.NewBuffer(body))
			req, err := messaging.BindBigQueryRequest(ctx)

			Expect(err).To(HaveOccurred())
			Expect(req).To(BeNil())
		})
	})

	Context("when the requestId is nil", func() {
		It("will return an error", func() {
			_, ctx := CreateBigQueryRequest(CreateBigQueryRequestBody([][]interface{}{{"1", "2", "3"}, {"4", "5", "6"}}, nil))

			req, err := messaging.BindBigQueryRequest(ctx)

			Expect(err).To(HaveOccurred())
			Expect(req).To(BeNil())
		})
	})
})
