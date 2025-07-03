package middleware_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/logging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/middleware"
	. "github.com/skyflowapi/skyflow-integrations/bigquery/common/test"
)

var (
	ctx      *gin.Context
	recorder *httptest.ResponseRecorder
	router   *gin.Engine
)

var _ = BeforeEach(func() {
	recorder = httptest.NewRecorder()
	ctx, router = gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", "/", nil)
})

func Chain(handlers ...gin.HandlerFunc) {
	router.GET("/", handlers...)
	router.HandleContext(ctx)
}

var _ = Describe("BigQuery error formatting", func() {
	Context("when there is no error set", func() {
		It("should not modify the response", func() {
			body := gin.H{"message": "test"}
			Chain(
				func(c *gin.Context) {
					Expect(recorder.Body.String()).To(Equal(""))
					Expect(recorder.Code).To(Equal(http.StatusOK))
					c.Next()
					Expect(recorder.Body.String()).To(Equal(MarshalAndStringify(body)))
					Expect(recorder.Code).To(Equal(http.StatusMultipleChoices))
				},
				middleware.BigQueryErrorMiddleware,
				func(c *gin.Context) {
					c.JSON(http.StatusMultipleChoices, body)
				},
			)
		})
	})

	Context("when there is an error set", func() {
		It("should use the BigQuery error response format", func() {
			Chain(
				func(c *gin.Context) {
					Expect(recorder.Body.String()).To(Equal(""))
					Expect(recorder.Code).To(Equal(http.StatusOK))
					c.Next()
					Expect(recorder.Body.String()).To(Equal(MarshalAndStringify(messaging.BigQueryError{ErrorMessage: "error encountered in proxy: test error"})))
					Expect(recorder.Code).To(Equal(http.StatusBadRequest))
				},
				middleware.BigQueryErrorMiddleware,
				func(c *gin.Context) {
					_ = c.AbortWithError(http.StatusBadRequest, errors.New("test error"))
				},
			)
		})
	})

	Context("when there are multiple errors set", func() {
		It("should use the BigQuery error response format for the last error", func() {
			Chain(
				func(c *gin.Context) {
					Expect(recorder.Body.String()).To(Equal(""))
					Expect(recorder.Code).To(Equal(http.StatusOK))
					c.Next()
					Expect(recorder.Body.String()).To(Equal(MarshalAndStringify(messaging.BigQueryError{ErrorMessage: "error encountered in proxy: test error 2"})))
					Expect(recorder.Code).To(Equal(http.StatusBadRequest))
				},
				middleware.BigQueryErrorMiddleware,
				func(c *gin.Context) {
					_ = c.Error(errors.New("test error 0"))
					_ = c.Error(errors.New("test error 1"))
					_ = c.AbortWithError(http.StatusBadRequest, errors.New("test error 2"))
				},
			)
		})
	})

	Context("when there is already a response written", func() {
		It("should not modify the response", func() {
			body := gin.H{"message": "test"}
			Chain(
				func(c *gin.Context) {
					Expect(recorder.Body.String()).To(Equal(""))
					Expect(recorder.Code).To(Equal(http.StatusOK))
					c.Next()
					Expect(recorder.Body.String()).To(Equal(MarshalAndStringify(body)))
					Expect(recorder.Code).To(Equal(http.StatusBadRequest))
				},
				middleware.BigQueryErrorMiddleware,
				func(c *gin.Context) {
					c.JSON(http.StatusBadRequest, body)
					_ = c.Error(errors.New("test error"))
				},
			)
		})
	})
})

var _ = Describe("Request ID assignment", func() {
	It("should set the request ID", func() {
		Chain(
			middleware.RequestIDAssignmentMiddleware,
			func(c *gin.Context) {
				Expect(c.GetString(middleware.RequestIDContextKey)).To(MatchRegexp("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
			},
		)
	})
})

var _ = Describe("Request idempotency key assignment", func() {
	Context("when the request is a valid BigQuery request", func() {
		It("should set the request idempotency key", func() {
			idempotencyKey := "test-idempotency-key"
			body := messaging.BigQueryRequest{
				RequestIdempotencyKey: &idempotencyKey,
			}
			bodyBytes, err := json.Marshal(body)
			Expect(err).To(BeNil())
			ctx.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			Chain(
				middleware.BigQueryRequestIdempotencyKeyAssignmentMiddleware,
				func(c *gin.Context) {
					Expect(c.GetString(middleware.RequestIdempotencyKeyContextKey)).To(Equal(idempotencyKey))
				},
			)
		})
	})

	Context("when the request is not a valid BigQuery request", func() {
		It("should not set the request idempotency key", func() {
			body := gin.H{"message": "test"}
			bodyBytes, err := json.Marshal(body)
			Expect(err).To(BeNil())
			ctx.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			Chain(
				func(c *gin.Context) {
					c.Next()
					Expect(ctx.Errors).To(HaveLen(1))
					Expect(ctx.Errors.Last()).To(MatchError(fmt.Errorf("invalid request format: missing requestId")))
				},
				middleware.BigQueryRequestIdempotencyKeyAssignmentMiddleware,
				func(c *gin.Context) {
					Expect(c.GetString(middleware.RequestIdempotencyKeyContextKey)).To(BeEmpty())
				},
			)
		})
	})
})

var _ = Describe("GCP request logger", func() {
	Describe("the request logger builder that is returned", func() {
		Context("when the GCP project ID is not set", func() {
			It("should add a timestamp, request ID, and request idempotency key to the logger", func() {
				mockLogger := &MockLogger{}
				builder := middleware.GCPRequestLoggerBuilder(mockLogger, "")

				requestID := "request-id"
				requestIdempotencyKey := "request-idempotency-key"
				ctx.Set(middleware.RequestIDContextKey, requestID)
				ctx.Set(middleware.RequestIdempotencyKeyContextKey, requestIdempotencyKey)

				builtLogger, ok := builder(ctx).(*MockLogger)
				Expect(ok).To(BeTrue())

				Expect(builtLogger.Context).To(HaveLen(3))
				_, err := time.Parse("2006/01/02 - 15:04:05", builtLogger.Context[0])
				Expect(err).To(BeNil())
				Expect(builtLogger.Context[1]).To(Equal(requestID))
				Expect(builtLogger.Context[2]).To(Equal(requestIdempotencyKey))
			})

			It("should warn that the GCP project ID is not set", func() {
				mockLogger := &MockLogger{}
				builder := middleware.GCPRequestLoggerBuilder(mockLogger, "")

				builtLogger, ok := builder(ctx).(*MockLogger)
				Expect(ok).To(BeTrue())

				Expect(builtLogger.Logs).To(HaveLen(1))
				Expect(builtLogger.Logs[0].Level).To(Equal(logging.WarnLevel))
				Expect(builtLogger.Logs[0].Messages).To(Equal([]string{"GCP project ID is not configured: the request logger will not be structured and will not include the GCP trace ID"}))
			})
		})

		Context("when the GCP project ID is set", func() {
			It("should structure the logger", func() {
				mockLogger := &MockLogger{}
				builder := middleware.GCPRequestLoggerBuilder(mockLogger, "test-project-id")

				requestID := "request-id"
				requestIdempotencyKey := "request-idempotency-key"
				ctx.Set(middleware.RequestIDContextKey, requestID)
				ctx.Set(middleware.RequestIdempotencyKeyContextKey, requestIdempotencyKey)
				ctx.Request.Header.Set("X-Cloud-Trace-Context", "6C26BC42CF33A48D29746CAF746FAF7B/7A6A9D96523F891AD4A2B2DBFDDDBA2EA57C6EFAC606A698462945845934CB2F/o=1")

				builtLogger, ok := builder(ctx).(*logging.GCPCloudRunStructuredLogger)
				Expect(ok).To(BeTrue())

				Expect(mockLogger.Context).To(HaveLen(3))
				_, err := time.Parse("2006/01/02 - 15:04:05", mockLogger.Context[0])
				Expect(err).To(BeNil())
				Expect(mockLogger.Context[1]).To(Equal(requestID))
				Expect(mockLogger.Context[2]).To(Equal(requestIdempotencyKey))
				Expect(builtLogger.TraceID).To(Equal("projects/test-project-id/traces/6C26BC42CF33A48D29746CAF746FAF7B"))
			})

			It("should not issue any logs", func() {
				mockLogger := &MockLogger{}
				builder := middleware.GCPRequestLoggerBuilder(mockLogger, "test-project-id")

				builtLogger, ok := builder(ctx).(*MockLogger)
				Expect(ok).To(BeTrue())

				Expect(builtLogger.Logs).To(BeEmpty())
			})
		})
	})
})

var _ = Describe("Assigning the request logger builder to the context", func() {
	It("should set the request logger builder in the context", func() {
		mockLogger := &MockLogger{}
		builder := func(c *gin.Context) logging.Logger {
			return mockLogger
		}
		Chain(
			middleware.RequestLoggerBuilderAssignmentMiddleware(builder),
			func(c *gin.Context) {
				maybeLoggerBuilder, ok := c.Get(middleware.RequestLoggerBuilderContextKey)
				Expect(ok).To(BeTrue())
				loggerBuilder, ok := maybeLoggerBuilder.(middleware.BuildRequestLogger)
				Expect(ok).To(BeTrue())
				logger := loggerBuilder(c)
				Expect(logger).To(Equal(mockLogger))
			},
		)
	})
})

var _ = Describe("Getting the request logger from the context", func() {
	It("should get the request logger from the context", func() {
		mockLogger := &MockLogger{}
		builder := func(c *gin.Context) logging.Logger {
			return mockLogger
		}
		Chain(
			middleware.RequestLoggerBuilderAssignmentMiddleware(builder),
			func(c *gin.Context) {
				logger, err := middleware.GetRequestLoggerFromContext(c)
				Expect(err).To(BeNil())
				Expect(logger).To(Equal(mockLogger))
			},
		)
	})

	Context("when the request logger builder is not set", func() {
		It("should return an error", func() {
			logger, err := middleware.GetRequestLoggerFromContext(ctx)
			Expect(logger).To(BeNil())
			Expect(err).To(MatchError(fmt.Errorf("missing %s in the server context", middleware.RequestLoggerBuilderContextKey)))
		})
	})

	Context("when the request logger builder is set but is not a function", func() {
		It("should return an error", func() {
			ctx.Set(middleware.RequestLoggerBuilderContextKey, "not a function")
			logger, err := middleware.GetRequestLoggerFromContext(ctx)
			Expect(logger).To(BeNil())
			Expect(err).To(MatchError(fmt.Errorf("expected %s in the server context to be a logger builder", middleware.RequestLoggerBuilderContextKey)))
		})
	})
})

var _ = Describe("Response logging", func() {
	Context("when the response is successful", func() {
		It("should log the response", func() {
			mockLogger := &MockLogger{}
			Chain(
				middleware.RequestLoggerBuilderAssignmentMiddleware(func(c *gin.Context) logging.Logger {
					return mockLogger
				}),
				middleware.ResponseLoggingMiddleware,
				func(c *gin.Context) {
					c.JSON(http.StatusOK, gin.H{"message": "test"})
				},
			)
			Expect(mockLogger.Logs).To(HaveLen(1))
			Expect(mockLogger.Logs[0].Level).To(Equal(logging.InfoLevel))
			Expect(mockLogger.Logs[0].Messages[0]).To(Equal(strconv.Itoa(http.StatusOK)))
			latency, err := time.ParseDuration(strings.TrimSpace(mockLogger.Logs[0].Messages[1]))
			Expect(err).To(BeNil())
			Expect(latency).To(BeNumerically(">", 0))
			Expect(mockLogger.Logs[0].Messages[2]).To(Equal("GET     \"/\"\n"))
		})
	})

	Context("when the response includes errors", func() {
		var mockLogger *MockLogger

		BeforeEach(func() {
			mockLogger = &MockLogger{}
		})

		handler := func(statusCode int, loggingLevel logging.Level) {
			Chain(
				middleware.RequestLoggerBuilderAssignmentMiddleware(func(c *gin.Context) logging.Logger {
					return mockLogger
				}),
				middleware.ResponseLoggingMiddleware,
				func(c *gin.Context) {
					_ = c.Error(errors.New("first error"))
					_ = c.AbortWithError(statusCode, errors.New("second error\nis a multiline error message"))
				},
			)
			Expect(mockLogger.Logs).To(HaveLen(1))
			Expect(mockLogger.Logs[0].Level).To(Equal(loggingLevel))
			Expect(mockLogger.Logs[0].Messages[0]).To(Equal(strconv.Itoa(statusCode)))
			latency, err := time.ParseDuration(strings.TrimSpace(mockLogger.Logs[0].Messages[1]))
			Expect(err).To(BeNil())
			Expect(latency).To(BeNumerically(">", 0))
			Expect(mockLogger.Logs[0].Messages[2]).To(Equal("GET     \"/\"\nError #01: first error\nError #02: second error\nis a multiline error message\n"))
		}

		entries := []any{handler}

		for i := range 500 {
			statusCode := i + http.StatusContinue
			var loggingLevel logging.Level
			if statusCode == http.StatusOK {
				loggingLevel = logging.InfoLevel
			} else {
				loggingLevel = logging.ErrorLevel
			}
			entries = append(entries, Entry(fmt.Sprintf("when the status code is %d", statusCode), statusCode, loggingLevel))
		}

		DescribeTable("logging the response", entries...)
	})

	Context("when the logger is not set", func() {
		It("should return an error", func() {
			Chain(
				func(c *gin.Context) {
					c.Next()
					Expect(ctx.Errors).To(HaveLen(1))
					// We have other tests to check the exact failure modes for getting the request logger from the context
					Expect(ctx.Errors.Last()).To(HaveOccurred())
				},
				middleware.ResponseLoggingMiddleware,
				func(c *gin.Context) {
					c.JSON(http.StatusOK, gin.H{"message": "test"})
				},
			)
		})
	})
})

var _ = Describe("Building the BigQuery middlewares", func() {
	It("should build the BigQuery middlewares", func() {
		mockLogger := &MockLogger{}
		mockLoggerBuilder := func(c *gin.Context) logging.Logger {
			return mockLogger
		}
		middlewares := middleware.BuildBigQueryMiddlewares(mockLoggerBuilder)
		Expect(middlewares).To(HaveLen(5))
		Expect(reflect.ValueOf(middlewares[0]).Pointer()).To(Equal(reflect.ValueOf(middleware.BigQueryErrorMiddleware).Pointer()))
		Expect(reflect.ValueOf(middlewares[1]).Pointer()).To(Equal(reflect.ValueOf(middleware.RequestIDAssignmentMiddleware).Pointer()))
		Expect(reflect.ValueOf(middlewares[2]).Type().String()).To(Equal(reflect.ValueOf(middleware.RequestLoggerBuilderAssignmentMiddleware(mockLoggerBuilder)).Type().String()))
		Expect(reflect.ValueOf(middlewares[3]).Pointer()).To(Equal(reflect.ValueOf(middleware.ResponseLoggingMiddleware).Pointer()))
		Expect(reflect.ValueOf(middlewares[4]).Pointer()).To(Equal(reflect.ValueOf(middleware.BigQueryRequestIdempotencyKeyAssignmentMiddleware).Pointer()))
	})
})
