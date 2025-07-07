// Copyright (c) 2025 Skyflow, Inc.

package messaging

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type GCPLogLevel string

// Should align with GCP's severity labels:
// https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#logseverity
const (
	GCPLogLevelEmergency GCPLogLevel = "EMERGENCY"
	GCPLogLevelError     GCPLogLevel = "ERROR"
	GCPLogLevelWarning   GCPLogLevel = "WARNING"
	GCPLogLevelInfo      GCPLogLevel = "INFO"
)

type GCPCloudRunLogEntry struct {
	Message  string `json:"message"`
	Severity string `json:"severity,omitempty"`
	Trace    string `json:"logging.googleapis.com/trace,omitempty"`
}

type GCPCloudTraceContext struct {
	headerValue string
}

func GCPCloudTraceContextFromHeader(header http.Header) (*GCPCloudTraceContext, error) {
	// https://cloud.google.com/run/docs/logging#writing_structured_logs
	const name = "X-Cloud-Trace-Context"
	value := header.Get(name)
	if value == "" {
		return nil, fmt.Errorf("header does not contain %s", name)
	}
	context := &GCPCloudTraceContext{
		headerValue: value,
	}
	return context, nil
}

func (c *GCPCloudTraceContext) TraceID(projectID string) (string, error) {
	if projectID == "" {
		return "", errors.New("projectID is required to derive the GCP cloud trace ID")
	}
	traceParts := strings.Split(c.headerValue, "/")
	if len(traceParts) <= 0 || len(traceParts[0]) <= 0 {
		return "", errors.New("could not parse GCP cloud trace context")
	}
	return fmt.Sprintf("projects/%s/traces/%s", projectID, traceParts[0]), nil
}
