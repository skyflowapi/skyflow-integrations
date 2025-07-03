package main_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"time"

	"github.com/gin-gonic/gin"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	skyflowErrors "github.com/skyflowapi/skyflow-go/commonutils/errors"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/batching"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/logging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging/skyflow"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/middleware"
	. "github.com/skyflowapi/skyflow-integrations/bigquery/common/test"
	main "github.com/skyflowapi/skyflow-integrations/bigquery/detokenize/cmd"
)

var _ = Describe("Creating a new state", func() {
	Context("when the environment variables are not set", func() {
		It("should return an error", func() {
			state, err := main.NewState()
			Expect(err).To(MatchError("SkyflowSACredentials: missing required value: SKYFLOW_SA_CREDENTIALS"))
			Expect(state.Logger).To(Not(BeNil()))
		})
	})

	Context("when the environment variables are set", func() {
		BeforeEach(func() {
			Expect(os.Setenv("SKYFLOW_SA_CREDENTIALS", "test-credentials")).To(BeNil())
			Expect(os.Setenv("SKYFLOW_VAULT_URL", "test-vault-url")).To(BeNil())
			Expect(os.Setenv("LOGGING_LEVEL", "warn")).To(BeNil())
			Expect(os.Setenv("GCP_PROJECT_ID", "test-project-id")).To(BeNil())
			Expect(os.Setenv("SKYFLOW_MAX_BATCH_SIZE", "100")).To(BeNil())
			Expect(os.Setenv("SKYFLOW_API_TIMEOUT_SECONDS", "250")).To(BeNil())
		})

		AfterEach(func() {
			Expect(os.Unsetenv("SKYFLOW_SA_CREDENTIALS")).To(BeNil())
			Expect(os.Unsetenv("SKYFLOW_VAULT_URL")).To(BeNil())
			Expect(os.Unsetenv("LOGGING_LEVEL")).To(BeNil())
			Expect(os.Unsetenv("GCP_PROJECT_ID")).To(BeNil())
			Expect(os.Unsetenv("SKYFLOW_MAX_BATCH_SIZE")).To(BeNil())
			Expect(os.Unsetenv("SKYFLOW_API_TIMEOUT_SECONDS")).To(BeNil())
		})

		It("should return the state", func() {
			wrapped := log.StandardLogger()
			state, err := main.NewState()
			Expect(err).To(BeNil())
			Expect(state.Logger).To(Not(BeNil()))
			Expect(wrapped.Level).To(Equal(log.WarnLevel))
			Expect(state.SkyflowSACredentials).To(Equal("test-credentials"))
			Expect(state.VaultURL).To(Equal("test-vault-url"))
			Expect(state.LoggingLevel).To(Equal("warn"))
			Expect(state.GCPProjectID).To(Equal("test-project-id"))
			Expect(state.MaxBatchSize).To(Equal(100))
			Expect(state.SkyflowAPITimeoutSeconds).To(Equal(250))
		})
	})

	Context("when the logging level is not valid", func() {
		BeforeEach(func() {
			Expect(os.Setenv("SKYFLOW_SA_CREDENTIALS", "test-credentials")).To(BeNil())
			Expect(os.Setenv("SKYFLOW_VAULT_URL", "test-vault-url")).To(BeNil())
			Expect(os.Setenv("LOGGING_LEVEL", "invalid")).To(BeNil())
			Expect(os.Setenv("GCP_PROJECT_ID", "test-project-id")).To(BeNil())
			Expect(os.Setenv("SKYFLOW_MAX_BATCH_SIZE", "100")).To(BeNil())
		})

		AfterEach(func() {
			Expect(os.Unsetenv("SKYFLOW_SA_CREDENTIALS")).To(BeNil())
			Expect(os.Unsetenv("SKYFLOW_VAULT_URL")).To(BeNil())
			Expect(os.Unsetenv("LOGGING_LEVEL")).To(BeNil())
			Expect(os.Unsetenv("GCP_PROJECT_ID")).To(BeNil())
			Expect(os.Unsetenv("SKYFLOW_MAX_BATCH_SIZE")).To(BeNil())
		})

		It("should return an error", func() {
			state, err := main.NewState()
			Expect(err).To(MatchError("not a valid logging level: \"invalid\""))
			Expect(state.Logger).To(Not(BeNil()))
		})
	})
})

var _ = Describe("Generating a bearer token", func() {
	Context("when the bearer token is expired", func() {
		It("should generate a new bearer token", func() {
			isBearerExpired := func(bearer string) bool {
				return true
			}
			bearerFromCredentials := func(credentials string) (string, error) {
				return "new-bearer", nil
			}
			state := main.State{
				Bearer:                "expired",
				SkyflowSACredentials:  "test-credentials",
				BearerFromCredentials: bearerFromCredentials,
				IsBearerExpired:       isBearerExpired,
			}
			bearer, err := state.GenerateBearer()
			Expect(err).To(BeNil())
			Expect(bearer).To(Equal("new-bearer"))
		})
	})

	Context("when the bearer token is not expired", func() {
		It("should not generate a new bearer token", func() {
			isBearerExpired := func(bearer string) bool {
				return false
			}
			bearerFromCredentials := func(credentials string) (string, error) {
				return "new-bearer", nil
			}
			state := main.State{
				Bearer:                "not-expired",
				SkyflowSACredentials:  "test-credentials",
				BearerFromCredentials: bearerFromCredentials,
				IsBearerExpired:       isBearerExpired,
			}
			bearer, err := state.GenerateBearer()
			Expect(err).To(BeNil())
			Expect(bearer).To(Equal("not-expired"))
		})
	})

	Context("when there is an error generating the bearer token", func() {
		It("should return an error", func() {
			isBearerExpired := func(bearer string) bool {
				return true
			}
			bearerFromCredentials := func(credentials string) (string, error) {
				return "", errors.New("error generating bearer token")
			}
			state := main.State{
				Bearer:                "expired",
				SkyflowSACredentials:  "test-credentials",
				BearerFromCredentials: bearerFromCredentials,
				IsBearerExpired:       isBearerExpired,
			}
			bearer, err := state.GenerateBearer()
			Expect(err).To(MatchError("failed to authenticate with the Skyflow API: error generating bearer token"))
			Expect(bearer).To(Equal(""))
		})
	})
})

var _ = Describe("Refreshing a bearer token", func() {
	Context("when the bearer token is expired", func() {
		It("should refresh the bearer token", func() {
			isBearerExpired := func(bearer string) bool {
				return true
			}
			bearerFromCredentials := func(credentials string) (string, error) {
				return "new-bearer", nil
			}
			state := main.State{
				Bearer:                "expired",
				SkyflowSACredentials:  "test-credentials",
				BearerFromCredentials: bearerFromCredentials,
				IsBearerExpired:       isBearerExpired,
			}
			err := state.RefreshBearer()
			Expect(err).To(BeNil())
			Expect(state.Bearer).To(Equal("new-bearer"))
		})
	})

	Context("when the bearer token is not expired", func() {
		It("should not refresh the bearer token", func() {
			isBearerExpired := func(bearer string) bool {
				return false
			}
			bearerFromCredentials := func(credentials string) (string, error) {
				return "new-bearer", nil
			}
			state := main.State{
				Bearer:                "not-expired",
				SkyflowSACredentials:  "test-credentials",
				BearerFromCredentials: bearerFromCredentials,
				IsBearerExpired:       isBearerExpired,
			}
			err := state.RefreshBearer()
			Expect(err).To(BeNil())
			Expect(state.Bearer).To(Equal("not-expired"))
		})
	})

	Context("when there is an error generating the bearer token", func() {
		It("should return an error", func() {
			isBearerExpired := func(bearer string) bool {
				return true
			}
			bearerFromCredentials := func(credentials string) (string, error) {
				skyflowError := skyflowErrors.NewSkyflowError(
					skyflowErrors.InvalidInput,
					"error-message",
				)
				return "", skyflowError
			}
			state := main.State{
				Bearer:                "expired",
				SkyflowSACredentials:  "test-credentials",
				BearerFromCredentials: bearerFromCredentials,
				IsBearerExpired:       isBearerExpired,
			}
			err := state.RefreshBearer()
			Expect(err).To(MatchError("failed to authenticate with the Skyflow API: Message: error-message, Original Error (if any): <nil>"))
			Expect(state.Bearer).To(Equal("expired"))
		})
	})
})

var _ = Describe("Logging replies", func() {
	Context("when there are no errors", func() {
		It("should log the number of successful replies", func() {
			replies := []skyflow.DetokenizeResponseObject{
				{HttpCode: ToPtr(http.StatusOK)},
				{HttpCode: ToPtr(http.StatusOK)},
				{HttpCode: ToPtr(http.StatusOK)},
				{HttpCode: ToPtr(http.StatusOK)},
			}
			logger := MockLogger{}
			main.LogReplies(&logger, replies)
			Expect(logger.Logs).To(HaveLen(1))
			Expect(logger.Logs[0].Level).To(Equal(logging.InfoLevel))
			Expect(logger.Logs[0].Messages).To(Equal([]string{"successfully detokenized 4 token(s)"}))
		})
	})

	Context("when there are errors", func() {
		It("should log the number of failed detokenizations", func() {
			replies := []skyflow.DetokenizeResponseObject{
				{HttpCode: ToPtr(http.StatusInternalServerError), Error: ToPtr("error-message-0-internal-server-error")},
				{HttpCode: ToPtr(http.StatusBadRequest)},
				{HttpCode: ToPtr(http.StatusOK)},
				{HttpCode: ToPtr(http.StatusOK)},
				{HttpCode: ToPtr(http.StatusBadGateway), Error: ToPtr("error-message-4-bad-gateway")},
				{HttpCode: ToPtr(http.StatusOK)},
				{HttpCode: ToPtr(http.StatusInternalServerError), Error: ToPtr("error-message-6-internal-server-error")},
				{HttpCode: ToPtr(http.StatusOK)},
				{HttpCode: ToPtr(http.StatusOK)},
				{HttpCode: ToPtr(http.StatusAlreadyReported), Error: ToPtr("error-message-8-already-reported")},
				{HttpCode: ToPtr(http.StatusOK)},
			}
			logger := MockLogger{}
			main.LogReplies(&logger, replies)
			Expect(logger.Logs).To(HaveLen(1))
			Expect(logger.Logs[0].Level).To(Equal(logging.WarnLevel))
			Expect(logger.Logs[0].Messages).To(Equal([]string{
				"failed to detokenize 5/11 token(s):\n" +
					"0: 500: error-message-0-internal-server-error\n" +
					"1: 400: detokenization failed but the error message is missing\n" +
					"4: 502: error-message-4-bad-gateway\n" +
					"6: 500: error-message-6-internal-server-error\n" +
					"9: 208: error-message-8-already-reported",
			}))
		})
	})
})

var _ = Describe("Submitting batches of tokens", func() {
	Context("when there are no errors", func() {
		It("should set the outputs of the batch", func() {
			expectedVaultID := "vault-id"
			expectedVaultHost := "vault-host"
			expectedVaultURL := "https://" + expectedVaultHost
			expectedBearer := "bearer"
			expectedTimeout := 1 * time.Second
			expectedTokensPointers := []*string{
				ToPtr("token-0"),
				nil,
				ToPtr("token-2"),
				ToPtr("token-3"),
				ToPtr("token-4"),
				ToPtr("token-5"),
			}
			expectedDetokens := []skyflow.DetokenizeResponseObject{
				{
					Token:          expectedTokensPointers[0],
					Value:          "value-0",
					HttpCode:       ToPtr(http.StatusOK),
					TokenGroupName: ToPtr("token-group-name-0"),
				},
				{
					Token:          nil,
					Value:          nil,
					HttpCode:       nil,
					TokenGroupName: nil,
				},
				{
					Token:          expectedTokensPointers[2],
					Value:          "value-2",
					HttpCode:       ToPtr(http.StatusBadRequest),
					TokenGroupName: ToPtr("token-group-name-2"),
				},
				{
					Token:          expectedTokensPointers[3],
					Value:          "value-3",
					HttpCode:       ToPtr(http.StatusOK),
					TokenGroupName: ToPtr("token-group-name-3"),
				},
				{
					Token:          expectedTokensPointers[4],
					Value:          "value-4",
					HttpCode:       ToPtr(http.StatusOK),
					TokenGroupName: ToPtr("token-group-name-4"),
				},
				{
					Token:          expectedTokensPointers[5],
					Value:          "value-5",
					HttpCode:       ToPtr(http.StatusConflict),
					TokenGroupName: ToPtr("token-group-name-5"),
				},
			}

			out := make([]skyflow.DetokenizeResponseObject, len(expectedTokensPointers))
			submitter := main.NewBatchSubmitter(out, http.DefaultClient, expectedTimeout, expectedVaultURL, expectedBearer)

			nSends := 0
			submitter.RequestSender = func(flow messaging.RequestFlow, exponentialBackoff *messaging.ExponentialBackoff) error {
				nSends++
				Expect(exponentialBackoff).To(Not(BeNil()))
				Expect(exponentialBackoff.MaxInterval).To(Equal(time.Duration(20) * time.Second))
				Expect(exponentialBackoff.InitialInterval).To(Equal(1 * time.Second))
				Expect(exponentialBackoff.RandomizationFactor).To(Equal(0.30))
				Expect(exponentialBackoff.Multiplier).To(Equal(2.0))
				Expect(exponentialBackoff.MaxTries).To(Equal(uint(3)))
				err := flow.Send()
				Expect(err).To(BeNil())
				return nil
			}

			mockClient := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					body, err := io.ReadAll(req.Body)
					Expect(err).To(BeNil())
					var payload map[string]interface{}
					Expect(json.Unmarshal(body, &payload)).To(BeNil())
					Expect(req.URL.Scheme).To(Equal("https"))
					Expect(req.URL.Host).To(Equal(expectedVaultHost))
					Expect(req.URL.Path).To(Equal("/v2/tokens/detokenize"))
					Expect(req.Method).To(Equal("POST"))
					Expect(req.Header.Get("Authorization")).To(Equal("Bearer " + expectedBearer))
					Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))

					nonNilTokensAsInterfaces := make([]interface{}, 0)
					nonNilDetokensAsPointers := make([]*skyflow.DetokenizeResponseObject, 0)
					for i := range expectedTokensPointers {
						if expectedTokensPointers[i] != nil {
							nonNilTokensAsInterfaces = append(nonNilTokensAsInterfaces, *expectedTokensPointers[i])
							nonNilDetokensAsPointers = append(nonNilDetokensAsPointers, &expectedDetokens[i])
						}
					}

					Expect(payload).To(Equal(map[string]interface{}{
						"vaultID": expectedVaultID,
						"tokens":  nonNilTokensAsInterfaces,
					}))
					return &http.Response{
						Body: io.NopCloser(bytes.NewBufferString(MarshalAndStringify(skyflow.DetokenizeResponse{
							Response: nonNilDetokensAsPointers,
						}))),
						StatusCode: http.StatusMultiStatus,
					}, nil
				},
			}

			submitter.RequestFlowFactory = func(client messaging.HttpDoer, timeout time.Duration, vaultID string, vaultURL string, bearer string, tokens []string) (*skyflow.DetokenizeFlow, error) {
				httpClient, ok := client.(*http.Client)
				Expect(ok).To(BeTrue())
				Expect(httpClient.Timeout).To(BeZero())
				Expect(timeout).To(Equal(expectedTimeout))
				Expect(vaultID).To(Equal(expectedVaultID))
				Expect(vaultURL).To(Equal(expectedVaultURL))
				Expect(bearer).To(Equal(expectedBearer))
				nonNilTokens := make([]string, 0)
				for i := range expectedTokensPointers {
					if expectedTokensPointers[i] != nil {
						nonNilTokens = append(nonNilTokens, *expectedTokensPointers[i])
					}
				}
				Expect(tokens).To(Equal(nonNilTokens))
				return skyflow.NewDetokenizeFlow(mockClient, timeout, vaultID, vaultURL, bearer, tokens)
			}

			indices := make([]int, len(expectedTokensPointers))
			for i := range indices {
				indices[i] = i
			}
			batch, err := batching.NewBatchFrom(indices, expectedTokensPointers)
			Expect(err).To(BeNil())
			err = submitter.Submit(main.BatchKey{VaultID: expectedVaultID}, *batch)

			Expect(err).To(BeNil())
			Expect(nSends).To(Equal(1))
			Expect(out).To(Equal(expectedDetokens))
		})
	})

	Context("when creating the request flow fails", func() {
		It("should return an error", func() {
			submitter := main.NewBatchSubmitter([]skyflow.DetokenizeResponseObject{}, http.DefaultClient, 1*time.Second, "invalid-vault-url", "bearer")
			batch, err := batching.NewBatchFrom([]int{0}, []*string{ToPtr("token-0")})
			Expect(err).To(BeNil())
			err = submitter.Submit(main.BatchKey{VaultID: "vault-id"}, *batch)
			Expect(err).To(MatchError("invalid vaultURL: must have scheme `https` or point to localhost"))
		})
	})

	Context("when the request flow fails", func() {
		It("should return an error", func() {
			submitter := main.NewBatchSubmitter([]skyflow.DetokenizeResponseObject{}, http.DefaultClient, 1*time.Second, "https://vault-host", "bearer")
			submitter.RequestSender = func(flow messaging.RequestFlow, exponentialBackoff *messaging.ExponentialBackoff) error {
				return errors.New("request flow failed")
			}
			batch, err := batching.NewBatchFrom([]int{0}, []*string{ToPtr("token-0")})
			Expect(err).To(BeNil())
			err = submitter.Submit(main.BatchKey{VaultID: "vault-id"}, *batch)
			Expect(err).To(MatchError("request flow failed"))
		})
	})

	Context("when the output size does not match the batch indices", func() {
		It("should return an error", func() {
			submitter := main.NewBatchSubmitter([]skyflow.DetokenizeResponseObject{}, http.DefaultClient, 1*time.Second, "https://vault-host", "bearer")
			mockClient := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						Body: io.NopCloser(bytes.NewBufferString(MarshalAndStringify(skyflow.DetokenizeResponse{
							Response: []*skyflow.DetokenizeResponseObject{
								{
									Token:          ToPtr("token-0"),
									Value:          "value-0",
									HttpCode:       ToPtr(http.StatusOK),
									TokenGroupName: ToPtr("token-group-name-0"),
								},
							},
						}))),
						StatusCode: http.StatusOK,
					}, nil
				},
			}
			submitter.RequestFlowFactory = func(client messaging.HttpDoer, timeout time.Duration, vaultID string, vaultURL string, bearer string, tokens []string) (*skyflow.DetokenizeFlow, error) {
				return skyflow.NewDetokenizeFlow(mockClient, timeout, vaultID, vaultURL, bearer, tokens)
			}
			batch, err := batching.NewBatchFrom([]int{0}, []*string{ToPtr("token-0")})
			Expect(err).To(BeNil())
			err = submitter.Submit(main.BatchKey{VaultID: "vault-id"}, *batch)
			Expect(err).To(MatchError("batch indices must match output size: found index 0, output size 0"))
		})
	})
})

var _ = Describe("Validating the number of call arguments", func() {
	Context("when the number of arguments is not correct", func() {
		It("should return an error", func() {
			err := main.ValidateNumberOfCallArguments([]interface{}{})
			Expect(err).To(MatchError("exactly 2 arguments are expected"))
		})
	})

	Context("when the number of arguments is correct", func() {
		It("should not return an error", func() {
			err := main.ValidateNumberOfCallArguments([]interface{}{"vault-id", "token-0"})
			Expect(err).To(BeNil())
		})
	})
})

var _ = Describe("Batch key getter", func() {
	Context("when the number of arguments is not correct", func() {
		It("should return an error", func() {
			batchKey, err := main.BatchKeyGetter{}.GetBatchKey([]interface{}{"vault-id"})
			Expect(err).To(MatchError("exactly 2 arguments are expected"))
			Expect(batchKey.VaultID).To(BeEmpty())
		})
	})

	Context("when the vault ID is not a string", func() {
		It("should return an error", func() {
			batchKey, err := main.BatchKeyGetter{}.GetBatchKey([]interface{}{1, "token-0"})
			Expect(err).To(MatchError("vaultId argument must be a string"))
			Expect(batchKey.VaultID).To(BeEmpty())
		})
	})

	It("should return the batch key", func() {
		batchKey, err := main.BatchKeyGetter{}.GetBatchKey([]interface{}{"vault-id", "token-0"})
		Expect(err).To(BeNil())
		Expect(batchKey.VaultID).To(Equal("vault-id"))
	})
})

var _ = Describe("Batch value getter", func() {
	Context("when the number of arguments is not correct", func() {
		It("should return an error", func() {
			value, err := main.BatchValueGetter{}.GetBatchValue([]interface{}{"vault-id"})
			Expect(err).To(MatchError("exactly 2 arguments are expected"))
			Expect(value).To(BeNil())
		})
	})

	Context("when the token is not a string", func() {
		It("should return an error", func() {
			value, err := main.BatchValueGetter{}.GetBatchValue([]interface{}{"vault-id", 1})
			Expect(err).To(MatchError("token argument must be a string"))
			Expect(value).To(BeNil())
		})
	})

	It("should return the batch value", func() {
		batchValue, err := main.BatchValueGetter{}.GetBatchValue([]interface{}{"vault-id", "token-0"})
		Expect(err).To(BeNil())
		Expect(*batchValue).To(Equal("token-0"))
	})

	Context("when the token is nil", func() {
		It("should return nil", func() {
			value, err := main.BatchValueGetter{}.GetBatchValue([]interface{}{"vault-id", nil})
			Expect(err).To(BeNil())
			Expect(value).To(BeNil())
		})
	})
})

var _ = Describe("Handler", func() {
	var (
		ctx               *gin.Context
		recorder          *httptest.ResponseRecorder
		router            *gin.Engine
		mockLogger        *MockLogger
		mockLoggerBuilder middleware.BuildRequestLogger
	)

	var _ = BeforeEach(func() {
		recorder = httptest.NewRecorder()
		ctx, router = gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest("POST", "/", nil)
		mockLogger = &MockLogger{}
		mockLoggerBuilder = func(c *gin.Context) logging.Logger {
			return mockLogger
		}
	})

	Chain := func(handlers ...gin.HandlerFunc) {
		router.POST("/", handlers...)
		router.HandleContext(ctx)
	}

	Context("when there is not a logger in the context", func() {
		It("should return an error", func() {
			handler := main.Handler(
				func(in [][]interface{}, out []skyflow.DetokenizeResponseObject) error {
					return nil
				},
				func(logger logging.Logger, replies []skyflow.DetokenizeResponseObject) {},
			)
			Chain(
				func(c *gin.Context) {
					c.Next()
					Expect(recorder.Code).To(Equal(messaging.StatusInternalServerErrorPermanent))
					Expect(c.Errors.Last().Error()).To(Equal("logger not found in server context"))
				},
				handler,
			)
		})
	})

	Context("when the request is invalid", func() {
		It("should return an error", func() {
			handler := main.Handler(
				func(in [][]interface{}, out []skyflow.DetokenizeResponseObject) error {
					return nil
				},
				func(logger logging.Logger, replies []skyflow.DetokenizeResponseObject) {},
			)
			Chain(
				middleware.RequestLoggerBuilderAssignmentMiddleware(mockLoggerBuilder),
				func(c *gin.Context) {
					c.Next()
					Expect(recorder.Code).To(Equal(http.StatusBadRequest))
					Expect(c.Errors.Last().Error()).To(Equal("invalid request format: request body does not match expected format: see https://cloud.google.com/bigquery/docs/remote-functions#input_format"))
				},
				handler,
			)
		})
	})

	Context("when the calls are empty", func() {
		It("should return an empty response", func() {
			handler := main.Handler(
				func(in [][]interface{}, out []skyflow.DetokenizeResponseObject) error {
					return nil
				},
				func(logger logging.Logger, replies []skyflow.DetokenizeResponseObject) {},
			)
			ctx.Request.Body = io.NopCloser(bytes.NewBufferString(MarshalAndStringify(messaging.BigQueryRequest{
				Calls:                 [][]interface{}{},
				RequestIdempotencyKey: ToPtr("request-idempotency-key"),
			})))
			Chain(
				middleware.RequestLoggerBuilderAssignmentMiddleware(mockLoggerBuilder),
				func(c *gin.Context) {
					c.Next()
					Expect(recorder.Code).To(Equal(http.StatusOK))
					Expect(recorder.Body.String()).To(Equal(MarshalAndStringify(messaging.BigQueryResponse[skyflow.DetokenizeResponseObject]{
						Replies: []skyflow.DetokenizeResponseObject{},
					})))
				},
				handler,
			)
		})
	})

	Context("when the detokenization fails", func() {
		It("should return an error", func() {
			handler := main.Handler(
				func(in [][]interface{}, out []skyflow.DetokenizeResponseObject) error {
					return errors.New("failed to authenticate with the Skyflow API: failed to generate bearer")
				},
				func(logger logging.Logger, replies []skyflow.DetokenizeResponseObject) {},
			)
			ctx.Request.Body = io.NopCloser(bytes.NewBufferString(MarshalAndStringify(messaging.BigQueryRequest{
				Calls:                 [][]interface{}{{"vault-id", "token-0"}},
				RequestIdempotencyKey: ToPtr("request-idempotency-key"),
			})))
			Chain(
				middleware.RequestLoggerBuilderAssignmentMiddleware(mockLoggerBuilder),
				func(c *gin.Context) {
					c.Next()
					Expect(recorder.Code).To(Equal(messaging.StatusInternalServerErrorPermanent))
					Expect(c.Errors.Last().Error()).To(Equal("detokenization failed: failed to authenticate with the Skyflow API: failed to generate bearer"))
				},
				handler,
			)
		})
	})

	It("should return the detokenized values", func() {
		handler := main.Handler(
			func(in [][]interface{}, out []skyflow.DetokenizeResponseObject) error {
				Expect(in).To(Equal([][]interface{}{{"vault-id", "token-0"}}))
				Expect(out).To(HaveLen(1))
				out[0] = skyflow.DetokenizeResponseObject{
					Token: ToPtr("token-0"),
					Value: "value-0",
				}
				return nil
			},
			func(logger logging.Logger, replies []skyflow.DetokenizeResponseObject) {},
		)
		ctx.Request.Body = io.NopCloser(bytes.NewBufferString(MarshalAndStringify(messaging.BigQueryRequest{
			Calls:                 [][]interface{}{{"vault-id", "token-0"}},
			RequestIdempotencyKey: ToPtr("request-idempotency-key"),
		})))
		Chain(
			middleware.RequestLoggerBuilderAssignmentMiddleware(mockLoggerBuilder),
			func(c *gin.Context) {
				c.Next()
				Expect(recorder.Code).To(Equal(http.StatusOK))
				Expect(recorder.Body.String()).To(Equal(MarshalAndStringify(messaging.BigQueryResponse[skyflow.DetokenizeResponseObject]{
					Replies: []skyflow.DetokenizeResponseObject{{Token: ToPtr("token-0"), Value: "value-0"}},
				})))
			},
			handler,
		)
	})

	It("should log the replies", func() {
		nLogCalls := 0
		handler := main.Handler(
			func(in [][]interface{}, out []skyflow.DetokenizeResponseObject) error {
				Expect(in).To(Equal([][]interface{}{{"vault-id", "token-0"}}))
				Expect(out).To(HaveLen(1))
				out[0] = skyflow.DetokenizeResponseObject{
					Token: ToPtr("token-0"),
					Value: "value-0",
				}
				return nil
			},
			func(logger logging.Logger, replies []skyflow.DetokenizeResponseObject) {
				nLogCalls++
				Expect(reflect.ValueOf(logger).Pointer()).To(Equal(reflect.ValueOf(mockLogger).Pointer()))
				Expect(replies).To(Equal([]skyflow.DetokenizeResponseObject{{Token: ToPtr("token-0"), Value: "value-0"}}))
			},
		)
		ctx.Request.Body = io.NopCloser(bytes.NewBufferString(MarshalAndStringify(messaging.BigQueryRequest{
			Calls:                 [][]interface{}{{"vault-id", "token-0"}},
			RequestIdempotencyKey: ToPtr("request-idempotency-key"),
		})))
		Chain(
			middleware.RequestLoggerBuilderAssignmentMiddleware(mockLoggerBuilder),
			handler,
		)
		Expect(nLogCalls).To(Equal(1))
	})
})

var _ = Describe("Detokenization", func() {
	Context("when the batcher cannot be created", func() {
		It("should return an error", func() {
			detokenize := main.Detokenize(
				func(out []skyflow.DetokenizeResponseObject) (*batching.Batcher[[]interface{}, main.BatchKey, *string], error) {
					return nil, errors.New("failed to create batcher")
				},
			)
			err := detokenize([][]interface{}{{"vault-id", "token-0"}}, []skyflow.DetokenizeResponseObject{{}})
			Expect(err).To(MatchError("failed to create batcher"))
		})
	})

	Context("when the batching fails", func() {
		It("should return an error", func() {
			detokenize := main.Detokenize(
				func(out []skyflow.DetokenizeResponseObject) (*batching.Batcher[[]interface{}, main.BatchKey, *string], error) {
					return batching.NewBatcher(
						&MockBatchSubmitter[main.BatchKey, *string]{
							DoSubmit: func(key main.BatchKey, batch batching.Batch[*string]) error {
								return errors.New("failed to submit batch")
							},
						},
						&MockBatchKeyGetter[[]interface{}, main.BatchKey]{
							DoGetBatchKey: func(input []interface{}) (main.BatchKey, error) {
								return main.BatchKey{VaultID: "vault-id"}, nil
							},
						},
						&MockBatchValueGetter[[]interface{}, *string]{
							DoGetBatchValue: func(input []interface{}) (*string, error) {
								return ToPtr("token-0"), nil
							},
						},
						100,
					), nil
				},
			)
			err := detokenize([][]interface{}{{"vault-id", "token-0"}}, []skyflow.DetokenizeResponseObject{{}})
			Expect(err).To(MatchError("error submitting batch: failed to submit batch"))
		})
	})

	Context("when the batching succeeds", func() {
		It("should return nil", func() {
			detokenize := main.Detokenize(
				func(out []skyflow.DetokenizeResponseObject) (*batching.Batcher[[]interface{}, main.BatchKey, *string], error) {
					return batching.NewBatcher(
						&MockBatchSubmitter[main.BatchKey, *string]{
							DoSubmit: func(key main.BatchKey, batch batching.Batch[*string]) error {
								return nil
							},
						},
						&MockBatchKeyGetter[[]interface{}, main.BatchKey]{
							DoGetBatchKey: func(input []interface{}) (main.BatchKey, error) {
								return main.BatchKey{VaultID: "vault-id"}, nil
							},
						},
						&MockBatchValueGetter[[]interface{}, *string]{
							DoGetBatchValue: func(input []interface{}) (*string, error) {
								return ToPtr("token-0"), nil
							},
						},
						100,
					), nil
				},
			)
			err := detokenize([][]interface{}{{"vault-id", "token-0"}}, []skyflow.DetokenizeResponseObject{{}})
			Expect(err).To(BeNil())
		})
	})
})

var _ = Describe("Creating a detokenization batcher", func() {
	Context("when the bearer cannot be generated", func() {
		It("should return an error", func() {
			batcherWithOutput := main.BatcherWithOutput(
				http.DefaultClient,
				1*time.Second,
				func() (string, error) {
					return "", errors.New("failed to generate bearer")
				},
				"https://vault-host",
				100,
			)
			batcher, err := batcherWithOutput([]skyflow.DetokenizeResponseObject{{}})
			Expect(err).To(MatchError("failed to generate bearer"))
			Expect(batcher).To(BeNil())
		})
	})

	It("should create a batcher", func() {
		batcherWithOutput := main.BatcherWithOutput(
			http.DefaultClient,
			1*time.Second,
			func() (string, error) {
				return "bearer", nil
			},
			"https://vault-host",
			100,
		)
		batcher, err := batcherWithOutput([]skyflow.DetokenizeResponseObject{{}})
		Expect(err).To(BeNil())
		Expect(batcher).To(Not(BeNil()))
	})
})
