package middleware

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/logging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
)

var (
	RequestIdempotencyKeyContextKey = "RequestIdempotencyKey"
	RequestIDContextKey             = "RequestID"
	RequestLoggerBuilderContextKey  = "RequestLoggerBuilder"
)

type BuildRequestLogger func(c *gin.Context) logging.Logger

func BigQueryErrorMiddleware(c *gin.Context) {
	c.Next()

	if len(c.Errors) > 0 && c.Writer.Size() == 0 {
		err := c.Errors.Last().Err
		status := c.Writer.Status()
		c.JSON(status, messaging.BigQueryError{ErrorMessage: fmt.Sprintf("error encountered in proxy: %s", err.Error())})
	}
}

func RequestIDAssignmentMiddleware(c *gin.Context) {
	c.Set(RequestIDContextKey, uuid.New().String())
	c.Next()
}

func BigQueryRequestIdempotencyKeyAssignmentMiddleware(c *gin.Context) {
	req, err := messaging.BindBigQueryRequest(c)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	c.Set(RequestIdempotencyKeyContextKey, *req.RequestIdempotencyKey)
	c.Next()
}

func GCPRequestLoggerBuilder(logger logging.Logger, gcpProjectID string) BuildRequestLogger {
	const timestampLayout = "2006/01/02 - 15:04:05"

	if gcpProjectID == "" {
		// This warning is important for production, as we want structured logs to be setup in the GCP console.
		// During local development, it is more convenient to not set the GCP project ID for prettier logs in the terminal.
		logger.Warn("GCP project ID is not configured: the request logger will not be structured and will not include the GCP trace ID")
	}

	return func(c *gin.Context) logging.Logger {
		timestamp := time.Now()
		requestID := c.GetString(RequestIDContextKey)
		requestIdempotencyKey := c.GetString(RequestIdempotencyKeyContextKey)

		contextualized := logger.WithContext(
			timestamp.Format(timestampLayout),
			requestID,
			requestIdempotencyKey,
		)

		if gcpProjectID != "" {
			if gcpTraceCtx, err := messaging.GCPCloudTraceContextFromHeader(c.Request.Header); err == nil {
				if gcpTraceID, err := gcpTraceCtx.TraceID(gcpProjectID); err == nil {
					contextualized = logging.NewGCPCloudRunStructuredLogger(gcpTraceID, contextualized)
				}
			}
		}

		return contextualized
	}
}

func RequestLoggerBuilderAssignmentMiddleware(requestLoggerBuilder BuildRequestLogger) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set(RequestLoggerBuilderContextKey, requestLoggerBuilder)
		c.Next()
	}
}

func GetRequestLoggerFromContext(c *gin.Context) (logging.Logger, error) {
	maybeLoggerBuilder, ok := c.Get(RequestLoggerBuilderContextKey)
	if !ok {
		return nil, fmt.Errorf("missing %s in the server context", RequestLoggerBuilderContextKey)
	}
	loggerBuilder, ok := maybeLoggerBuilder.(BuildRequestLogger)
	if !ok {
		return nil, fmt.Errorf("expected %s in the server context to be a logger builder", RequestLoggerBuilderContextKey)
	}
	logger := loggerBuilder(c)
	return logger, nil
}

func ResponseLoggingMiddleware(c *gin.Context) {
	start := time.Now()

	c.Next()

	stop := time.Now()
	latency := stop.Sub(start)
	statusCode := c.Writer.Status()
	method := c.Request.Method
	path := c.Request.URL.Path
	errorMessage := c.Errors.ByType(gin.ErrorTypePrivate)

	logger, err := GetRequestLoggerFromContext(c)
	if err != nil {
		_ = c.AbortWithError(messaging.StatusInternalServerErrorPermanent, err)
		return
	}

	var handler func(messages ...string)
	if statusCode == http.StatusOK {
		handler = logger.Info
	} else {
		handler = logger.Error
	}

	handler(
		fmt.Sprintf("%3d", statusCode),
		fmt.Sprintf("%13v", latency),
		fmt.Sprintf("%-7s %#v\n%s",
			method,
			path,
			errorMessage,
		),
	)
}

func BuildBigQueryMiddlewares(requestLoggerBuilder BuildRequestLogger) gin.HandlersChain {
	return gin.HandlersChain{
		BigQueryErrorMiddleware,
		RequestIDAssignmentMiddleware,
		RequestLoggerBuilderAssignmentMiddleware(requestLoggerBuilder),
		ResponseLoggingMiddleware,
		BigQueryRequestIdempotencyKeyAssignmentMiddleware,
	}
}
