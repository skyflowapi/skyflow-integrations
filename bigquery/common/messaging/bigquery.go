// Copyright (c) 2025 Skyflow, Inc.

package messaging

import (
	"errors"

	"github.com/gin-gonic/gin"
)

const (
	// Custom 5xx HTTP error code to avoid retries from BigQuery.
	// BigQuery automatically retries the following error codes: 408, 429, 500, 503 and 504.
	StatusInternalServerErrorPermanent = 599
)

// BigQueryRequest defines the structure of the JSON request body. More details can be found here - https://cloud.google.com/bigquery/docs/remote-functions#input_format
type BigQueryRequest struct {
	Calls                 [][]interface{} `json:"calls"`
	RequestIdempotencyKey *string         `json:"requestId"`
}

func BindBigQueryRequest(c *gin.Context) (*BigQueryRequest, error) {
	var req BigQueryRequest
	if err := c.ShouldBindBodyWithJSON(&req); err != nil {
		return nil, errors.New("invalid request format: request body does not match expected format: see https://cloud.google.com/bigquery/docs/remote-functions#input_format")
	}
	if req.RequestIdempotencyKey == nil {
		return nil, errors.New("invalid request format: missing requestId")
	}
	return &req, nil
}

// BigQueryResponse defines the structure of the JSON response body. More details can be found here - https://cloud.google.com/bigquery/docs/remote-functions#output_format
type BigQueryResponse[T interface{}] struct {
	Replies []T `json:"replies"`
}

// BigQueryError defines the structure of the JSON response body. More details can be found here - https://cloud.google.com/bigquery/docs/remote-functions#output_format
type BigQueryError struct {
	ErrorMessage string `json:"errorMessage"`
}
