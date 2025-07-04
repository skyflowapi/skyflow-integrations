package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sethvargo/go-envconfig"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/batching"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/logging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging/skyflow"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/middleware"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/routing"
)

func LogReplies(logger logging.Logger, replies []skyflow.DetokenizeResponseObject) {
	var errorMessages []string

	for i, detoken := range replies {
		if detoken.HttpCode != nil && *detoken.HttpCode == http.StatusOK {
			continue
		}
		var errorMessage string
		if detoken.Error != nil {
			errorMessage = *detoken.Error
		} else {
			errorMessage = "detokenization failed but the error message is missing"
		}
		var statusCodeMessage string
		if detoken.HttpCode != nil {
			statusCodeMessage = strconv.Itoa(*detoken.HttpCode)
		}
		errorMessage = fmt.Sprintf("%d: %s: %s", i, statusCodeMessage, errorMessage)
		errorMessages = append(errorMessages, errorMessage)
	}

	if len(errorMessages) == 0 {
		logger.Info(fmt.Sprintf("successfully detokenized %d token(s)", len(replies)))
	} else {
		logger.Warn(fmt.Sprintf("failed to detokenize %d/%d token(s):\n%s", len(errorMessages), len(replies), strings.Join(errorMessages, "\n")))
	}
}

type BatchKey struct {
	VaultID string
}

type BatchSubmitter struct {
	out                []skyflow.DetokenizeResponseObject
	client             messaging.HttpDoer
	timeout            time.Duration
	vaultURL           string
	bearer             string
	RequestFlowFactory skyflow.DetokenizeFlowFactory
	RequestSender      messaging.RequestSender
}

func NewBatchSubmitter(out []skyflow.DetokenizeResponseObject, client messaging.HttpDoer, timeout time.Duration, vaultURL string, bearer string) BatchSubmitter {
	return BatchSubmitter{
		out:                out,
		client:             client,
		timeout:            timeout,
		vaultURL:           vaultURL,
		bearer:             bearer,
		RequestFlowFactory: skyflow.NewDetokenizeFlow,
		RequestSender:      messaging.SendRequest,
	}
}

// Submits a batch of tokens for detokenization.
// Returns an error if any unrecoverable error occurs within the proxy (not the downstream Skyflow API).
func (submitter BatchSubmitter) Submit(key BatchKey, batch batching.Batch[*string]) error {
	indices, tokens := submitter.submitAndRemoveNullTokens(batch)

	flow, err := submitter.RequestFlowFactory(submitter.client, submitter.timeout, key.VaultID, submitter.vaultURL, submitter.bearer, tokens)
	if err != nil {
		return err
	}

	if err := submitter.RequestSender(flow, messaging.DefaultExponentialBackoff()); err != nil {
		return err
	}

	detokens := flow.Results()

	for i := range detokens {
		if indices[i] < 0 || indices[i] >= len(submitter.out) {
			return fmt.Errorf("batch indices must match output size: found index %d, output size %d", indices[i], len(submitter.out))
		}
		submitter.out[indices[i]] = detokens[i]
	}
	return nil
}

// Null tokens are removed from the returned list of indices and tokens.
// Null tokens have their outputs set to a default response.
// The token strings are deferenced before being returned as there are no longer any null pointers.
func (submitter BatchSubmitter) submitAndRemoveNullTokens(batch batching.Batch[*string]) ([]int, []string) {
	indices := make([]int, 0)
	tokens := make([]string, 0)
	for i := range batch.Size() {
		if batch.Values[i] != nil {
			indices = append(indices, batch.Indices[i])
			tokens = append(tokens, *batch.Values[i])
		} else {
			// If the token is null, set the response accordingly
			submitter.out[batch.Indices[i]] = skyflow.DetokenizeResponseObject{}
		}
	}
	return indices, tokens
}

func ValidateNumberOfCallArguments(call []interface{}) error {
	const nargs = 2
	if len(call) != nargs {
		return fmt.Errorf("exactly %d arguments are expected", nargs)
	}
	return nil
}

type BatchKeyGetter struct{}

func (getter BatchKeyGetter) GetBatchKey(call []interface{}) (BatchKey, error) {
	batchKey := BatchKey{}
	if err := ValidateNumberOfCallArguments(call); err != nil {
		return batchKey, err
	}
	var ok bool
	batchKey.VaultID, ok = call[0].(string)
	if !ok {
		return batchKey, fmt.Errorf("vaultId argument must be a string")
	}
	return batchKey, nil
}

type BatchValueGetter struct{}

func (getter BatchValueGetter) GetBatchValue(call []interface{}) (*string, error) {
	if err := ValidateNumberOfCallArguments(call); err != nil {
		return nil, err
	}
	if call[1] == nil {
		return nil, nil
	}
	token, ok := call[1].(string)
	if !ok {
		return nil, fmt.Errorf("token argument must be a string")
	}
	return &token, nil
}

func Handler(
	detokenize func(in [][]interface{}, out []skyflow.DetokenizeResponseObject) error,
	logReplies func(logger logging.Logger, replies []skyflow.DetokenizeResponseObject),
) gin.HandlerFunc {
	return func(c *gin.Context) {
		logger, err := middleware.GetRequestLoggerFromContext(c)
		if err != nil {
			_ = c.AbortWithError(messaging.StatusInternalServerErrorPermanent, errors.New("logger not found in server context"))
			return
		}

		req, err := messaging.BindBigQueryRequest(c)
		if err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, err)
			return
		}

		res := messaging.BigQueryResponse[skyflow.DetokenizeResponseObject]{
			Replies: make([]skyflow.DetokenizeResponseObject, len(req.Calls)),
		}

		if len(req.Calls) == 0 {
			c.JSON(http.StatusOK, res)
			logger.Info("received request with empty calls; returning response with empty replies")
			return
		}

		if err := detokenize(req.Calls, res.Replies); err != nil {
			_ = c.AbortWithError(messaging.StatusInternalServerErrorPermanent, fmt.Errorf("detokenization failed: %v", err))
			return
		}

		c.JSON(http.StatusOK, res)
		logReplies(logger, res.Replies)
	}
}

type State struct {
	MaxBatchSize         int `env:"SKYFLOW_MAX_BATCH_SIZE, default=1000"`
	Bearer               string
	SkyflowSACredentials string `env:"SKYFLOW_SA_CREDENTIALS, required"`
	VaultURL             string `env:"SKYFLOW_VAULT_URL, required"`
	// Skyflow API timeout defaults to 5 minutes.
	// BigQuery gives 20 minutes to respond.
	SkyflowAPITimeoutSeconds int `env:"SKYFLOW_API_TIMEOUT_SECONDS, default=300"`
	Logger                   logging.Logger
	LoggingLevel             string `env:"LOGGING_LEVEL, default=WARN"`
	// If provided, the request logger will be structured and will include the GCP trace ID.
	// This is important for production, as we want structured logs to be setup in the GCP console.
	// During local development, it is more convenient to not set the GCP project ID for prettier logs in the terminal.
	GCPProjectID          string `env:"GCP_PROJECT_ID"`
	BearerFromCredentials skyflow.BearerFromCredentialsGenerator
	IsBearerExpired       skyflow.IsBearerExpiredChecker
}

func (state *State) GenerateBearer() (string, error) {
	if err := state.RefreshBearer(); err != nil {
		return "", err
	}
	return state.Bearer, nil
}

func (state *State) RefreshBearer() error {
	bearerGenerator := func() (string, error) {
		return state.BearerFromCredentials(state.SkyflowSACredentials)
	}
	bearer, err := skyflow.GenerateBearerIfExpired(state.Bearer, bearerGenerator, state.IsBearerExpired)
	if err != nil {
		return err
	}
	state.Bearer = bearer
	return nil
}

func NewState() (State, error) {
	rootLogger := logging.NewRootLogger()

	state := State{
		Logger:                rootLogger.WithContext("DETOKENIZE"),
		BearerFromCredentials: skyflow.BearerFromCredentials,
		IsBearerExpired:       skyflow.IsBearerExpired,
	}

	if err := envconfig.Process(context.Background(), &state); err != nil {
		return state, err
	}

	level, err := logging.ParseLevel(state.LoggingLevel)
	if err != nil {
		return state, err
	}
	rootLogger.SetLevel(level)

	return state, nil
}

func Detokenize(
	batcherWithOutput func(out []skyflow.DetokenizeResponseObject) (*batching.Batcher[[]interface{}, BatchKey, *string], error),
) func(in [][]interface{}, out []skyflow.DetokenizeResponseObject) error {
	return func(in [][]interface{}, out []skyflow.DetokenizeResponseObject) error {
		batcher, err := batcherWithOutput(out)
		if err != nil {
			return err
		}
		if err := batcher.Batch(in); err != nil {
			return err
		}
		return nil
	}
}

func BatcherWithOutput(
	client messaging.HttpDoer,
	timeout time.Duration,
	generateBearer skyflow.BearerGenerator,
	vaultURL string,
	maxBatchSize int,
) func(out []skyflow.DetokenizeResponseObject) (*batching.Batcher[[]interface{}, BatchKey, *string], error) {
	return func(out []skyflow.DetokenizeResponseObject) (*batching.Batcher[[]interface{}, BatchKey, *string], error) {
		bearer, err := generateBearer()
		if err != nil {
			return nil, err
		}
		batcher := batching.NewBatcher(
			NewBatchSubmitter(out, client, timeout, vaultURL, bearer),
			BatchKeyGetter{},
			BatchValueGetter{},
			maxBatchSize,
		)
		return batcher, nil
	}
}

func main() {
	state, err := NewState()
	if err != nil {
		state.Logger.Fatal(fmt.Sprintf("server cannot start: %v", err))
	}
	router, err := routing.CreateRouter()
	if err != nil {
		state.Logger.Fatal(fmt.Sprintf("server cannot start: %v", err))
	}
	requestLoggerBuilder := middleware.GCPRequestLoggerBuilder(state.Logger, state.GCPProjectID)
	handlers := gin.HandlersChain(middleware.BuildBigQueryMiddlewares(requestLoggerBuilder))
	detokenizeHandler := Handler(
		Detokenize(BatcherWithOutput(
			http.DefaultClient,
			time.Duration(state.SkyflowAPITimeoutSeconds)*time.Second,
			state.GenerateBearer,
			state.VaultURL,
			state.MaxBatchSize,
		)),
		LogReplies)
	handlers = append(handlers, detokenizeHandler)
	router.POST("/", handlers...)
	if err := router.Run(":8080"); err != nil {
		state.Logger.Fatal(fmt.Sprintf("server cannot start: %v", err))
	}
}
