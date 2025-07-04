package skyflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	saUtil "github.com/skyflowapi/skyflow-go/serviceaccount/util"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
)

type BearerGenerator func() (string, error)
type BearerFromCredentialsGenerator func(string) (string, error)
type IsBearerExpiredChecker func(string) bool

func BearerFromCredentials(credentials string) (string, error) {
	token, err := saUtil.GenerateBearerTokenFromCreds(credentials)
	if err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

func IsBearerExpired(bearer string) bool {
	return saUtil.IsExpired(bearer)
}

func GenerateBearerIfExpired(
	bearer string,
	generateBearer BearerGenerator,
	isBearerExpired IsBearerExpiredChecker,
) (string, error) {
	if isBearerExpired(bearer) {
		newBearer, err := generateBearer()
		if err != nil {
			return "", fmt.Errorf("failed to authenticate with the Skyflow API: %v", err)
		}
		return newBearer, nil
	}
	return bearer, nil
}

type BatchFlowHandler[I interface{}, O interface{}] interface {
	BuildPayload([]I) interface{}
	SetError(o *O, error error, statusCode int)
	DecodeBatch(size int, body []byte) ([]*O, error)
	DecodeError(body []byte) error
	ShouldRetry(o *O) bool
}

type BatchFlow[I interface{}, O interface{}] struct {
	client   messaging.HttpDoer
	method   string
	vaultURL string
	route    string
	bearer   string
	handler  BatchFlowHandler[I, O]
	timeout  time.Duration
	in       []I
	out      []O
	// Contains the indices of in/out that have not yet been successfully processed
	indices []int
}

func NewBatchFlow[I interface{}, O interface{}](
	client messaging.HttpDoer,
	timeout time.Duration,
	method string,
	vaultURL string,
	route string,
	bearer string,
	handler BatchFlowHandler[I, O],
	in []I,
) (*BatchFlow[I, O], error) {
	parsedVaultURL, err := url.Parse(vaultURL)
	if err != nil {
		return nil, fmt.Errorf("invalid vaultURL: %v", err)
	}
	if host, _, err := net.SplitHostPort(parsedVaultURL.Host); parsedVaultURL.Scheme != "https" && parsedVaultURL.Host != "localhost" && (err != nil || (host != "localhost" && host != "127.0.0.1")) {
		return nil, fmt.Errorf("invalid vaultURL: must have scheme `https` or point to localhost")
	}
	if parsedVaultURL.Host == "" {
		return nil, fmt.Errorf("invalid vaultURL: must have host")
	}

	out := make([]O, len(in))

	indices := make([]int, len(in))
	for i := range indices {
		indices[i] = i
	}

	flow := &BatchFlow[I, O]{
		client:   client,
		method:   method,
		vaultURL: vaultURL,
		route:    route,
		bearer:   bearer,
		handler:  handler,
		timeout:  timeout,
		in:       in,
		out:      out,
		indices:  indices,
	}

	return flow, nil
}

func (flow *BatchFlow[I, O]) Send() error {
	ctx, cancel := context.WithTimeout(context.Background(), flow.timeout)
	defer cancel()

	req, err := flow.buildRequest(ctx)
	if err != nil {
		// Unrecoverable: failed to build request
		flow.setErrorOnRemainingIndices(err, http.StatusInternalServerError)
		flow.clearRemainingIndices()
		return err
	}
	if req == nil {
		return nil
	}

	res, err := flow.client.Do(req)
	if err != nil {
		flow.setErrorOnRemainingIndices(err, http.StatusInternalServerError)
		if urlErr, ok := err.(*url.Error); ok && urlErr.Timeout() {
			// If the request timed out, do not retry
			flow.clearRemainingIndices()
		}
		return nil
	}
	defer res.Body.Close() //nolint:errcheck

	if err := flow.unpackResponse(res); err != nil {
		// Unrecoverable: failed to decode response
		flow.setErrorOnRemainingIndices(err, http.StatusInternalServerError)
		flow.clearRemainingIndices()
		return err
	}

	return nil
}

func (flow *BatchFlow[I, O]) IsDone() bool {
	// Done when there are no unprocessed indices
	return len(flow.indices) == 0
}

func (flow *BatchFlow[I, O]) buildRequest(ctx context.Context) (*http.Request, error) {
	if len(flow.indices) == 0 {
		return nil, nil
	}

	var in []I
	if len(flow.in) == len(flow.indices) {
		in = flow.in
	} else {
		in = make([]I, len(flow.indices))
		for i := range flow.indices {
			in[i] = flow.in[flow.indices[i]]
		}
	}

	payload := flow.handler.BuildPayload(in)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal body of Skyflow API request: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, flow.method, flow.vaultURL+flow.route, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create Skyflow API request: %v", err)
	}
	req.Header.Add("Content-Type", gin.MIMEJSON)
	req.Header.Add("Authorization", "Bearer "+flow.bearer)

	return req, nil
}

func (flow *BatchFlow[I, O]) unpackResponse(response *http.Response) error {
	body, err := io.ReadAll(response.Body)

	if err != nil {
		// If the response body could not be read, this suggests an unexpected,
		// likely transient, error occurred - therefore, we should retry.
		err = fmt.Errorf("failed to read Skyflow API response body: %v", err)
		flow.setErrorOnRemainingIndices(err, http.StatusInternalServerError)
		return nil
	}

	switch {
	case messaging.IsSuccessfulStatusCode(response.StatusCode):
		out, err := flow.decodeBatch(body)
		if err != nil {
			// If a successful response cannot be decoded, this indicates a breaking
			// change in the response format that we should fail hard on.
			return err
		}
		flow.unpackBatch(out)
		return nil
	case messaging.IsServerErrorStatusCode(response.StatusCode):
		out, err := flow.decodeBatch(body)
		if err != nil {
			flow.setErrorOnRemainingIndices(err, response.StatusCode)
			return nil
		}
		flow.unpackBatch(out)
		return nil
	case response.StatusCode == http.StatusTooManyRequests:
		err := flow.decodeError(body)
		flow.setErrorOnRemainingIndices(err, response.StatusCode)
		return nil
	default:
		err := flow.decodeError(body)
		flow.setErrorOnRemainingIndices(err, response.StatusCode)
		flow.clearRemainingIndices()
		return nil
	}
}

func (flow *BatchFlow[I, O]) unpackBatch(out []*O) {
	newIndices := make([]int, 0)
	for i, o := range out {
		flow.out[flow.indices[i]] = *o
		if flow.handler.ShouldRetry(o) {
			newIndices = append(newIndices, flow.indices[i])
		}
	}
	flow.indices = newIndices
}

func (flow *BatchFlow[I, O]) decodeBatch(body []byte) ([]*O, error) {
	out, err := flow.handler.DecodeBatch(len(flow.indices), body)
	if err != nil {
		if decodedErr := flow.handler.DecodeError(body); decodedErr != nil {
			err = decodedErr
		} else {
			err = fmt.Errorf("failed to decode Skyflow API response: while decoding '%s': %v", string(body), err)
		}
	}
	return out, err
}

func (flow *BatchFlow[I, O]) decodeError(body []byte) error {
	err := flow.handler.DecodeError(body)
	if err == nil {
		err = errors.New(string(body))
	}
	return err
}

func (flow *BatchFlow[I, O]) setErrorOnRemainingIndices(error error, statusCode int) {
	for _, index := range flow.indices {
		flow.handler.SetError(&flow.out[index], error, statusCode)
	}
}

func (flow *BatchFlow[I, O]) clearRemainingIndices() {
	flow.indices = flow.indices[:0]
}

func (flow *BatchFlow[I, O]) Results() []O {
	return flow.out
}
