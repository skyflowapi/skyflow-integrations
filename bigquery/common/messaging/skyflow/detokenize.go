// Copyright (c) 2025 Skyflow, Inc.

package skyflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
)

type DetokenizeResponse struct {
	Response []*DetokenizeResponseObject `json:"response,omitempty"`
}

type DetokenizeResponseObject struct {
	// Token to be detokenized
	Token *string     `json:"token,omitempty"`
	Value interface{} `json:"value,omitempty"`
	// Token group name
	TokenGroupName *string `json:"tokenGroupName,omitempty"`
	// Error if detokenization failed
	Error *string `json:"error,omitempty"`
	// HTTP status code of the response
	HttpCode *int `json:"httpCode,omitempty"`
	// Additional metadata associated with the token, such as tableName or skyflowID
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type DetokenizeErrorResponse struct {
	Error *DetokenizeErrorResponseObject `json:"error,omitempty"`
}

type DetokenizeErrorResponseObject struct {
	Message *string `json:"message,omitempty"`
}

type detokenizeFlowHandler struct {
	vaultID string
}

func (handler *detokenizeFlowHandler) BuildPayload(tokens []string) interface{} {
	return map[string]interface{}{
		"vaultID": handler.vaultID,
		"tokens":  tokens,
	}
}

func (handler *detokenizeFlowHandler) SetError(
	detoken *DetokenizeResponseObject,
	error error,
	statusCode int,
) {
	if detoken == nil {
		return
	}
	var errorMessage *string
	if error != nil {
		errorString := error.Error()
		errorMessage = &errorString
	}
	detoken.Error = errorMessage
	detoken.HttpCode = &statusCode
}

func (handler *detokenizeFlowHandler) DecodeBatch(
	size int,
	body []byte,
) ([]*DetokenizeResponseObject, error) {
	detokens := make([]*DetokenizeResponseObject, size)
	resBody := DetokenizeResponse{Response: detokens}
	if err := json.Unmarshal(body, &resBody); err != nil {
		return nil, err
	}
	if len(detokens) > 0 && detokens[len(detokens)-1] == nil {
		return nil, fmt.Errorf("received fewer tokens than requested")
	}
	return detokens, nil
}

func (handler *detokenizeFlowHandler) DecodeError(body []byte) error {
	var resBody DetokenizeErrorResponse
	if err := json.Unmarshal(body, &resBody); err == nil && resBody.Error != nil && resBody.Error.Message != nil && *resBody.Error.Message != "" {
		return errors.New(*resBody.Error.Message)
	}
	return nil
}

func (handler *detokenizeFlowHandler) ShouldRetry(detoken *DetokenizeResponseObject) bool {
	return detoken != nil && detoken.HttpCode != nil && (messaging.IsServerErrorStatusCode(*detoken.HttpCode) ||
		*detoken.HttpCode == http.StatusTooManyRequests)
}

type DetokenizeFlow = BatchFlow[string, DetokenizeResponseObject]

type DetokenizeFlowFactory func(messaging.HttpDoer, time.Duration, string, string, string, []string) (*DetokenizeFlow, error)

func NewDetokenizeFlow(
	client messaging.HttpDoer,
	timeout time.Duration,
	vaultID string,
	vaultURL string,
	bearer string,
	tokens []string,
) (*DetokenizeFlow, error) {
	return NewBatchFlow(
		client,
		timeout,
		"POST",
		vaultURL,
		"/v2/tokens/detokenize",
		bearer,
		&detokenizeFlowHandler{vaultID: vaultID},
		tokens,
	)
}
