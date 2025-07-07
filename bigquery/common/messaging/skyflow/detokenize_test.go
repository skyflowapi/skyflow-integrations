// Copyright (c) 2025 Skyflow, Inc.

package skyflow_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging/skyflow"
	. "github.com/skyflowapi/skyflow-integrations/bigquery/common/test"
)

var _ = Describe("Detokenize flow", func() {
	It("will create the correct request", func() {
		vaultID := "vaultID"
		scheme := "https"
		vaultHost := "vault-host"
		bearer := "bearer"
		tokens := []string{"token1", "token2"}
		client := &MockHttpClient{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				GinkgoHelper()
				body, err := io.ReadAll(req.Body)
				Expect(err).To(BeNil())
				var payload map[string]interface{}
				Expect(json.Unmarshal(body, &payload)).To(BeNil())
				Expect(req.URL.Scheme).To(Equal(scheme))
				Expect(req.URL.Host).To(Equal(vaultHost))
				Expect(req.URL.Path).To(Equal("/v2/tokens/detokenize"))
				Expect(req.Method).To(Equal("POST"))
				Expect(req.Header.Get("Authorization")).To(Equal("Bearer " + bearer))
				Expect(req.Header.Get("Content-Type")).To(Equal("application/json"))
				Expect(payload).To(Equal(map[string]interface{}{
					"vaultID": vaultID,
					"tokens":  []interface{}{tokens[0], tokens[1]},
				}))
				return &http.Response{
					Body: io.NopCloser(bytes.NewBufferString("")),
				}, nil
			},
		}
		flow, err := skyflow.NewDetokenizeFlow(
			client,
			1*time.Second,
			vaultID,
			scheme+"://"+vaultHost,
			bearer,
			tokens,
		)
		Expect(err).To(BeNil())
		err = flow.Send()
		Expect(err).To(BeNil())
	})

	Context("when the tokens are empty", func() {
		It("will not send a request", func() {
			nCalls := 0
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					nCalls++
					return &http.Response{
						Body: io.NopCloser(bytes.NewBufferString("")),
					}, nil
				},
			}
			flow, err := skyflow.NewDetokenizeFlow(
				client,
				1*time.Second,
				"vaultID",
				"https://vault-host",
				"bearer",
				[]string{},
			)
			Expect(err).To(BeNil())
			err = flow.Send()
			Expect(err).To(BeNil())
			Expect(nCalls).To(BeZero())
		})
	})

	Context("when the first request is sent", func() {
		It("will send all of the tokens", func() {
			tokens := []string{"token1", "token2"}
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					GinkgoHelper()
					body, err := io.ReadAll(req.Body)
					Expect(err).To(BeNil())
					var payload map[string]interface{}
					Expect(json.Unmarshal(body, &payload)).To(BeNil())
					Expect(payload).To(Equal(map[string]interface{}{
						"vaultID": "vaultID",
						"tokens":  []interface{}{tokens[0], tokens[1]},
					}))
					return &http.Response{
						Body: io.NopCloser(bytes.NewBufferString("")),
					}, nil
				},
			}
			flow, err := skyflow.NewDetokenizeFlow(
				client,
				1*time.Second,
				"vaultID",
				"https://vault-host",
				"bearer",
				tokens,
			)
			Expect(err).To(BeNil())
			err = flow.Send()
			Expect(err).To(BeNil())
		})
	})

	Context("when creating the flow fails", func() {
		Context("when the vault URL is empty", func() {
			It("will return an error", func() {
				flow, err := skyflow.NewDetokenizeFlow(
					nil,
					1*time.Second,
					"vaultID",
					"",
					"bearer",
					[]string{"token"},
				)
				Expect(err).To(MatchError("invalid vaultURL: must have scheme `https` or point to localhost"))
				Expect(flow).To(BeNil())
			})
		})

		Context("when the vault URL is missing the scheme", func() {
			It("will return an error", func() {
				flow, err := skyflow.NewDetokenizeFlow(
					nil,
					1*time.Second,
					"vaultID",
					"vault-host",
					"bearer",
					[]string{"token"},
				)
				Expect(err).To(MatchError("invalid vaultURL: must have scheme `https` or point to localhost"))
				Expect(flow).To(BeNil())
			})
		})

		Context("when the vault URL is missing the host", func() {
			It("will return an error", func() {
				flow, err := skyflow.NewDetokenizeFlow(
					nil,
					1*time.Second,
					"vaultID",
					"https://",
					"bearer",
					[]string{"token"},
				)
				Expect(err).To(MatchError("invalid vaultURL: must have host"))
				Expect(flow).To(BeNil())
			})
		})

		Context("when the vault URL contains a control character", func() {
			It("will return an error", func() {
				flow, err := skyflow.NewDetokenizeFlow(
					nil,
					1*time.Second,
					"vaultID",
					"https://vault-host\x00",
					"bearer",
					[]string{"token"},
				)
				Expect(err).To(MatchError("invalid vaultURL: parse \"https://vault-host\\x00\": net/url: invalid control character in URL"))
				Expect(flow).To(BeNil())
			})
		})
	})

	Context("when a successful response is received", func() {
		Context("when a single token is detokenized", func() {
			It("will return a single detokenized token and the flow will be marked as successful", func() {
				value := "value"
				token := "token"
				httpCode := http.StatusOK
				tokenGroupName := "token-group-name"
				resBody := MarshalAndStringify(skyflow.DetokenizeResponse{
					Response: []*skyflow.DetokenizeResponseObject{
						{
							Token:          &token,
							Value:          &value,
							HttpCode:       &httpCode,
							TokenGroupName: &tokenGroupName,
						},
					},
				})
				client := &MockHttpClient{
					DoFunc: func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewBufferString(resBody)),
						}, nil
					},
				}
				flow, err := skyflow.NewDetokenizeFlow(
					client,
					1*time.Second,
					"vaultID",
					"https://vault-host",
					"bearer",
					[]string{token},
				)
				Expect(err).To(BeNil())

				err = flow.Send()

				Expect(err).To(BeNil())
				Expect(flow.IsDone()).To(BeTrue())
				results := flow.Results()
				Expect(results).To(HaveLen(1))
				Expect(results[0].Error).To(BeNil())
				Expect(*results[0].HttpCode).To(Equal(httpCode))
				Expect(*results[0].Token).To(Equal(token))
				Expect(results[0].Value).To(Equal(value))
				Expect(*results[0].TokenGroupName).To(Equal(tokenGroupName))
			})
		})

		Context("when multiple tokens are detokenized", func() {
			It("will return a list of detokenized tokens and the flow will be marked as successful", func() {
				tokens := []string{"token1", "token2"}
				values := []string{"value1", "value2"}
				tokenGroupNames := []string{"token-group-name1", "token-group-name2"}
				httpCodes := []int{http.StatusOK, http.StatusOK}
				resBody := MarshalAndStringify(skyflow.DetokenizeResponse{
					Response: []*skyflow.DetokenizeResponseObject{
						{
							Token:          &tokens[0],
							Value:          &values[0],
							HttpCode:       &httpCodes[0],
							TokenGroupName: &tokenGroupNames[0],
						},
						{
							Token:          &tokens[1],
							Value:          &values[1],
							HttpCode:       &httpCodes[1],
							TokenGroupName: &tokenGroupNames[1],
						},
					},
				})
				client := &MockHttpClient{
					DoFunc: func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewBufferString(resBody)),
						}, nil
					},
				}
				flow, err := skyflow.NewDetokenizeFlow(
					client,
					1*time.Second,
					"vaultID",
					"https://vault-host",
					"bearer",
					tokens,
				)
				Expect(err).To(BeNil())

				err = flow.Send()

				Expect(err).To(BeNil())
				Expect(flow.IsDone()).To(BeTrue())
				results := flow.Results()
				Expect(results).To(HaveLen(2))
				for i := range results {
					Expect(results[i].Error).To(BeNil())
					Expect(*results[i].HttpCode).To(Equal(httpCodes[i]))
					Expect(*results[i].Token).To(Equal(tokens[i]))
					Expect(results[i].Value).To(Equal(values[i]))
					Expect(*results[i].TokenGroupName).To(Equal(tokenGroupNames[i]))
				}
			})
		})
	})

	Context("when the response cannot be decoded after sending the request", func() {
		Context("when there are fewer tokens than requested", func() {
			Context("when the response is a 2xx", func() {
				var (
					flow *skyflow.DetokenizeFlow
					err  error
				)

				BeforeEach(func() {
					var err_ error
					resBody := MarshalAndStringify(skyflow.DetokenizeResponse{
						Response: []*skyflow.DetokenizeResponseObject{},
					})
					client := &MockHttpClient{
						DoFunc: func(req *http.Request) (*http.Response, error) {
							return &http.Response{
								StatusCode: http.StatusOK,
								Body:       io.NopCloser(bytes.NewBufferString(resBody)),
							}, nil
						},
					}
					flow, err_ = skyflow.NewDetokenizeFlow(
						client,
						1*time.Second,
						"vaultID",
						"https://vault-host",
						"bearer",
						[]string{"token"},
					)
					Expect(err_).To(BeNil())
					err = flow.Send()
				})

				It("will return an error", func() {
					Expect(err).To(MatchError("failed to decode Skyflow API response: while decoding '{}': received fewer tokens than requested"))
				})

				It("will set the error on the response object", func() {
					results := flow.Results()
					Expect(results).To(HaveLen(1))
					Expect(*results[0].Error).To(Equal("failed to decode Skyflow API response: while decoding '{}': received fewer tokens than requested"))
					Expect(*results[0].HttpCode).To(Equal(http.StatusInternalServerError))
				})

				It("will mark the flow as done", func() {
					Expect(flow.IsDone()).To(BeTrue())
				})
			})

			Context("when the response is a 5xx", func() {
				var (
					flow *skyflow.DetokenizeFlow
					err  error
				)

				BeforeEach(func() {
					var err_ error
					resBody := MarshalAndStringify(skyflow.DetokenizeResponse{
						Response: []*skyflow.DetokenizeResponseObject{},
					})
					client := &MockHttpClient{
						DoFunc: func(req *http.Request) (*http.Response, error) {
							return &http.Response{
								StatusCode: http.StatusInternalServerError,
								Body:       io.NopCloser(bytes.NewBufferString(resBody)),
							}, nil
						},
					}
					flow, err_ = skyflow.NewDetokenizeFlow(
						client,
						1*time.Second,
						"vaultID",
						"https://vault-host",
						"bearer",
						[]string{"token"},
					)
					Expect(err_).To(BeNil())
					err = flow.Send()
				})

				It("will not return an error", func() {
					Expect(err).To(BeNil())
				})

				It("will not mark the flow as done", func() {
					Expect(flow.IsDone()).To(BeFalse())
				})

				It("will set the error on the response object", func() {
					results := flow.Results()
					Expect(results).To(HaveLen(1))
					Expect(*results[0].Error).To(Equal("failed to decode Skyflow API response: while decoding '{}': received fewer tokens than requested"))
					Expect(*results[0].HttpCode).To(Equal(http.StatusInternalServerError))
				})
			})
		})

		Context("when the response is not valid JSON", func() {
			Context("when the response is a 2xx", func() {
				var (
					flow *skyflow.DetokenizeFlow
					err  error
				)

				BeforeEach(func() {
					var err_ error
					client := &MockHttpClient{
						DoFunc: func(req *http.Request) (*http.Response, error) {
							return &http.Response{
								StatusCode: http.StatusOK,
								Body:       io.NopCloser(bytes.NewBufferString("{")),
							}, nil
						},
					}
					flow, err_ = skyflow.NewDetokenizeFlow(
						client,
						1*time.Second,
						"vaultID",
						"https://vault-host",
						"bearer",
						[]string{"token"},
					)
					Expect(err_).To(BeNil())
					err = flow.Send()
				})

				It("will return an error", func() {
					Expect(err).To(MatchError("failed to decode Skyflow API response: while decoding '{': unexpected end of JSON input"))
				})

				It("will set the error on the response object", func() {
					results := flow.Results()
					Expect(results).To(HaveLen(1))
					Expect(*results[0].Error).To(Equal("failed to decode Skyflow API response: while decoding '{': unexpected end of JSON input"))
					Expect(*results[0].HttpCode).To(Equal(http.StatusInternalServerError))
				})

				It("will mark the flow as done", func() {
					Expect(flow.IsDone()).To(BeTrue())
				})
			})

			Context("when the response is a 5xx", func() {
				var (
					flow *skyflow.DetokenizeFlow
					err  error
				)

				BeforeEach(func() {
					var err_ error
					client := &MockHttpClient{
						DoFunc: func(req *http.Request) (*http.Response, error) {
							return &http.Response{
								StatusCode: http.StatusInternalServerError,
								Body:       io.NopCloser(bytes.NewBufferString("{")),
							}, nil
						},
					}
					flow, err_ = skyflow.NewDetokenizeFlow(
						client,
						1*time.Second,
						"vaultID",
						"https://vault-host",
						"bearer",
						[]string{"token"},
					)
					Expect(err_).To(BeNil())
					err = flow.Send()
				})

				It("will not return an error", func() {
					Expect(err).To(BeNil())
				})

				It("will not mark the flow as done", func() {
					Expect(flow.IsDone()).To(BeFalse())
				})

				It("will set the error on the response object", func() {
					results := flow.Results()
					Expect(results).To(HaveLen(1))
					Expect(*results[0].Error).To(Equal("failed to decode Skyflow API response: while decoding '{': unexpected end of JSON input"))
					Expect(*results[0].HttpCode).To(Equal(http.StatusInternalServerError))
				})
			})
		})
	})

	Context("when the request cannot be sent", func() {
		var (
			flow *skyflow.DetokenizeFlow
			err  error
		)

		BeforeEach(func() {
			var err_ error
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return nil, errors.New("test error")
				},
			}
			flow, err_ = skyflow.NewDetokenizeFlow(
				client,
				1*time.Second,
				"vaultID",
				"https://vault-host",
				"bearer",
				[]string{"token"},
			)
			Expect(err_).To(BeNil())
			err = flow.Send()
		})

		It("will not return an error", func() {
			Expect(err).To(BeNil())
		})

		It("will not mark the flow as done", func() {
			Expect(flow.IsDone()).To(BeFalse())
		})

		It("will set the error on the response object", func() {
			results := flow.Results()
			Expect(results).To(HaveLen(1))
			Expect(*results[0].Error).To(Equal("test error"))
			Expect(*results[0].HttpCode).To(Equal(http.StatusInternalServerError))
		})
	})

	Context("when the request timeout is exceeded", func() {
		It("will return an error", func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/v2/tokens/detokenize", func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(2 * time.Second)
			})
			testServer := httptest.NewServer(mux)
			defer testServer.Close()
			client := testServer.Client()
			flow, err := skyflow.NewDetokenizeFlow(
				client,
				1*time.Second,
				"vaultID",
				testServer.URL,
				"bearer",
				[]string{"token"},
			)
			Expect(err).To(BeNil())
			err = flow.Send()
			Expect(err).To(BeNil())
			Expect(flow.IsDone()).To(BeTrue())
			Expect(flow.Results()).To(HaveLen(1))
			Expect(*flow.Results()[0].Error).To(Equal(fmt.Sprintf("Post \"%s/v2/tokens/detokenize\": context deadline exceeded", testServer.URL)))
			Expect(*flow.Results()[0].HttpCode).To(Equal(http.StatusInternalServerError))
		})
	})

	It("will close the response body", func() {
		bodyString := "test error"
		mockReadCloser := &MockReadCloser{
			ReadCloser: io.NopCloser(bytes.NewBufferString(bodyString)),
		}
		client := &MockHttpClient{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       mockReadCloser,
				}, nil
			},
		}
		flow, err := skyflow.NewDetokenizeFlow(
			client,
			1*time.Second,
			"vaultID",
			"https://vault-host",
			"bearer",
			[]string{"token"},
		)
		Expect(err).To(BeNil())
		err = flow.Send()
		Expect(err).To(BeNil())
		Expect(*flow.Results()[0].Error).To(Equal(bodyString))
		Expect(mockReadCloser.NCloseCalls).To(Equal(1))
		Expect(mockReadCloser.NReadCalls).To(BeNumerically(">=", 1))
	})

	Context("when the error message format is recognized", func() {
		handler := func(inStatusCode int, outStatusCode int, isReturnedError bool) {
			errorMessage := "test error"
			bodyString := MarshalAndStringify(skyflow.DetokenizeErrorResponse{
				Error: &skyflow.DetokenizeErrorResponseObject{
					Message: ToPtr(errorMessage),
				},
			})
			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: inStatusCode,
						Body:       io.NopCloser(bytes.NewBufferString(bodyString)),
					}, nil
				},
			}
			flow, err := skyflow.NewDetokenizeFlow(
				client,
				1*time.Second,
				"vaultID",
				"https://vault-host",
				"bearer",
				[]string{"token"},
			)
			Expect(err).To(BeNil())

			err = flow.Send()
			if isReturnedError {
				Expect(err).To(MatchError(errors.New(errorMessage)))
			} else {
				Expect(err).To(BeNil())
			}
			Expect(*flow.Results()[0].Error).To(Equal(errorMessage))
			Expect(*flow.Results()[0].HttpCode).To(Equal(outStatusCode))
		}

		args := make([]any, 0)
		args = append(args, handler)

		for i := range 500 {
			inStatusCode := 100 + i
			isReturnedError := messaging.IsSuccessfulStatusCode(inStatusCode)
			outStatusCode := inStatusCode
			if messaging.IsSuccessfulStatusCode(inStatusCode) {
				outStatusCode = http.StatusInternalServerError
			}
			args = append(args, Entry(fmt.Sprintf("when the status code is %d", inStatusCode), inStatusCode, outStatusCode, isReturnedError))
		}

		DescribeTable("parsing the error message", args...)
	})

	Context("when handling single-item responses", func() {
		const (
			token                       = "token"
			value                       = "value"
			tokenGroupName              = "token-group-name"
			arbitraryBody               = "arbitrary-body"
			undecodableBody             = "undecodable-body"
			undecodableBodyErrorMessage = "failed to decode Skyflow API response: while decoding 'undecodable-body': invalid character 'u' looking for beginning of value"
			serverErrorBodyErrorMessage = "a server error occurred"
		)

		successBody := func(statusCode int) string {
			return MarshalAndStringify(skyflow.DetokenizeResponse{
				Response: []*skyflow.DetokenizeResponseObject{
					{
						Token:          ToPtr(token),
						Value:          value,
						TokenGroupName: ToPtr(tokenGroupName),
						HttpCode:       ToPtr(statusCode),
					},
				},
			})
		}

		serverErrorBody := func(statusCode int) string {
			return MarshalAndStringify(skyflow.DetokenizeResponse{
				Response: []*skyflow.DetokenizeResponseObject{
					{
						Error:    ToPtr(serverErrorBodyErrorMessage),
						HttpCode: ToPtr(statusCode),
					},
				},
			})
		}

		DescribeTable("status code handling",
			func(
				statusCode int,
				body string,
				returnedError error,
				responseObjectError *string,
				responseObjectStatusCode int,
				isDone bool,
			) {
				client := &MockHttpClient{
					DoFunc: func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: statusCode,
							Body:       io.NopCloser(bytes.NewBufferString(body)),
						}, nil
					},
				}
				flow, err := skyflow.NewDetokenizeFlow(
					client,
					1*time.Second,
					"vaultID",
					"https://vault-host",
					"bearer",
					[]string{"token"},
				)
				Expect(err).To(BeNil())
				err = flow.Send()
				if returnedError == nil {
					Expect(err).To(BeNil())
				} else {
					Expect(err).To(MatchError(returnedError))
				}
				Expect(flow.IsDone()).To(Equal(isDone))
				results := flow.Results()
				Expect(results).To(HaveLen(1))
				Expect(results[0].Error).To(Equal(responseObjectError))
				Expect(*results[0].HttpCode).To(Equal(responseObjectStatusCode))
				if responseObjectError != nil {
					Expect(results[0].Token).To(BeNil())
					Expect(results[0].Value).To(BeNil())
					Expect(results[0].TokenGroupName).To(BeNil())
				} else {
					Expect(*results[0].Token).To(Equal(token))
					Expect(results[0].Value).To(Equal(value))
					Expect(*results[0].TokenGroupName).To(Equal(tokenGroupName))
				}
			},
			// 1xx and 3xx: these status codes should not occur; if they do,
			// 	- do not return an error
			// 	- set the response body as the error on the response object
			// 	- set the response object status code to the status code of the response
			// 	- do not retry (done)
			Entry("when the status code is 100", http.StatusContinue, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusContinue, true),
			Entry("when the status code is 101", http.StatusSwitchingProtocols, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusSwitchingProtocols, true),
			Entry("when the status code is 102", http.StatusProcessing, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusProcessing, true),
			Entry("when the status code is 103", http.StatusEarlyHints, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusEarlyHints, true),
			Entry("when the status code is 300", http.StatusMultipleChoices, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusMultipleChoices, true),
			Entry("when the status code is 301", http.StatusMovedPermanently, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusMovedPermanently, true),
			Entry("when the status code is 302", http.StatusFound, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusFound, true),
			Entry("when the status code is 303", http.StatusSeeOther, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusSeeOther, true),
			Entry("when the status code is 304", http.StatusNotModified, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusNotModified, true),
			Entry("when the status code is 305", http.StatusUseProxy, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusUseProxy, true),
			Entry("when the status code is 307", http.StatusTemporaryRedirect, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusTemporaryRedirect, true),
			Entry("when the status code is 308", http.StatusPermanentRedirect, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusPermanentRedirect, true),
			// 2xx: these status codes are expected and indicate success (since the response is single-item).
			// If the response body is successfully decoded,
			//	- do not return an error
			//	- copy the response object
			//	- do not retry (done)
			Entry("when the status code is 200", http.StatusOK, successBody(http.StatusOK), nil, nil, http.StatusOK, true),
			Entry("when the status code is 201", http.StatusCreated, successBody(http.StatusCreated), nil, nil, http.StatusCreated, true),
			Entry("when the status code is 202", http.StatusAccepted, successBody(http.StatusAccepted), nil, nil, http.StatusAccepted, true),
			Entry("when the status code is 203", http.StatusNonAuthoritativeInfo, successBody(http.StatusNonAuthoritativeInfo), nil, nil, http.StatusNonAuthoritativeInfo, true),
			Entry("when the status code is 204", http.StatusNoContent, successBody(http.StatusNoContent), nil, nil, http.StatusNoContent, true),
			Entry("when the status code is 205", http.StatusResetContent, successBody(http.StatusResetContent), nil, nil, http.StatusResetContent, true),
			Entry("when the status code is 206", http.StatusPartialContent, successBody(http.StatusPartialContent), nil, nil, http.StatusPartialContent, true),
			Entry("when the status code is 207", http.StatusMultiStatus, successBody(http.StatusMultiStatus), nil, nil, http.StatusMultiStatus, true),
			Entry("when the status code is 208", http.StatusAlreadyReported, successBody(http.StatusAlreadyReported), nil, nil, http.StatusAlreadyReported, true),
			// If the response body cannot be decoded, this indicates a breaking change in the response format.
			// Therefore,
			//	- return an error (there has been a breaking change)
			//	- set the response body as the error on the response object
			//	- set the response object status code to 500 to indicate an internal server error occurred
			//	- do not retry (done)
			Entry("when the status code is 200 but the response body cannot be decoded", http.StatusOK, undecodableBody, errors.New(undecodableBodyErrorMessage), ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, true),
			Entry("when the status code is 201 but the response body cannot be decoded", http.StatusCreated, undecodableBody, errors.New(undecodableBodyErrorMessage), ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, true),
			Entry("when the status code is 202 but the response body cannot be decoded", http.StatusAccepted, undecodableBody, errors.New(undecodableBodyErrorMessage), ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, true),
			Entry("when the status code is 203 but the response body cannot be decoded", http.StatusNonAuthoritativeInfo, undecodableBody, errors.New(undecodableBodyErrorMessage), ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, true),
			Entry("when the status code is 204 but the response body cannot be decoded", http.StatusNoContent, undecodableBody, errors.New(undecodableBodyErrorMessage), ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, true),
			Entry("when the status code is 205 but the response body cannot be decoded", http.StatusResetContent, undecodableBody, errors.New(undecodableBodyErrorMessage), ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, true),
			Entry("when the status code is 206 but the response body cannot be decoded", http.StatusPartialContent, undecodableBody, errors.New(undecodableBodyErrorMessage), ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, true),
			Entry("when the status code is 207 but the response body cannot be decoded", http.StatusMultiStatus, undecodableBody, errors.New(undecodableBodyErrorMessage), ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, true),
			Entry("when the status code is 208 but the response body cannot be decoded", http.StatusAlreadyReported, undecodableBody, errors.New(undecodableBodyErrorMessage), ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, true),
			// 4xx (except for 429): these status codes are expected and indicate a client error; therefore,
			//	- do not return an error
			//	- set the response body as the error on the response object
			//	- set the response object status code to the status code of the response
			//	- do not retry (done)
			Entry("when the status code is 400", http.StatusBadRequest, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusBadRequest, true),
			Entry("when the status code is 401", http.StatusUnauthorized, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusUnauthorized, true),
			Entry("when the status code is 403", http.StatusForbidden, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusForbidden, true),
			Entry("when the status code is 404", http.StatusNotFound, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusNotFound, true),
			Entry("when the status code is 405", http.StatusMethodNotAllowed, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusMethodNotAllowed, true),
			Entry("when the status code is 406", http.StatusNotAcceptable, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusNotAcceptable, true),
			Entry("when the status code is 407", http.StatusProxyAuthRequired, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusProxyAuthRequired, true),
			Entry("when the status code is 408", http.StatusRequestTimeout, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusRequestTimeout, true),
			Entry("when the status code is 409", http.StatusConflict, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusConflict, true),
			Entry("when the status code is 410", http.StatusGone, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusGone, true),
			Entry("when the status code is 411", http.StatusLengthRequired, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusLengthRequired, true),
			Entry("when the status code is 412", http.StatusPreconditionFailed, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusPreconditionFailed, true),
			Entry("when the status code is 413", http.StatusRequestEntityTooLarge, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusRequestEntityTooLarge, true),
			Entry("when the status code is 414", http.StatusRequestURITooLong, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusRequestURITooLong, true),
			Entry("when the status code is 415", http.StatusUnsupportedMediaType, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusUnsupportedMediaType, true),
			Entry("when the status code is 416", http.StatusRequestedRangeNotSatisfiable, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusRequestedRangeNotSatisfiable, true),
			Entry("when the status code is 417", http.StatusExpectationFailed, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusExpectationFailed, true),
			Entry("when the status code is 418", http.StatusTeapot, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusTeapot, true),
			Entry("when the status code is 421", http.StatusMisdirectedRequest, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusMisdirectedRequest, true),
			Entry("when the status code is 422", http.StatusUnprocessableEntity, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusUnprocessableEntity, true),
			Entry("when the status code is 423", http.StatusLocked, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusLocked, true),
			Entry("when the status code is 424", http.StatusFailedDependency, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusFailedDependency, true),
			Entry("when the status code is 425", http.StatusTooEarly, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusTooEarly, true),
			Entry("when the status code is 426", http.StatusUpgradeRequired, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusUpgradeRequired, true),
			Entry("when the status code is 428", http.StatusPreconditionRequired, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusPreconditionRequired, true),
			Entry("when the status code is 431", http.StatusRequestHeaderFieldsTooLarge, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusRequestHeaderFieldsTooLarge, true),
			Entry("when the status code is 451", http.StatusUnavailableForLegalReasons, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusUnavailableForLegalReasons, true),
			// 429: this status code is expected and indicates a rate limit was exceeded; therefore,
			//	- do not return an error
			//	- set the response body as the error on the response object
			//	- set the response object status code to the status code of the response
			//	- retry (not done)
			Entry("when the status code is 429", http.StatusTooManyRequests, arbitraryBody, nil, ToPtr(arbitraryBody), http.StatusTooManyRequests, false),
			// 5xx: these status codes are expected and indicate a server error.
			// If the response body is successfully decoded,
			//	- do not return an error
			//	- copy the response object
			//	- retry (not done)
			Entry("when the status code is 500", http.StatusInternalServerError, serverErrorBody(http.StatusInternalServerError), nil, ToPtr(serverErrorBodyErrorMessage), http.StatusInternalServerError, false),
			Entry("when the status code is 501", http.StatusNotImplemented, serverErrorBody(http.StatusNotImplemented), nil, ToPtr(serverErrorBodyErrorMessage), http.StatusNotImplemented, false),
			Entry("when the status code is 502", http.StatusBadGateway, serverErrorBody(http.StatusBadGateway), nil, ToPtr(serverErrorBodyErrorMessage), http.StatusBadGateway, false),
			Entry("when the status code is 503", http.StatusServiceUnavailable, serverErrorBody(http.StatusServiceUnavailable), nil, ToPtr(serverErrorBodyErrorMessage), http.StatusServiceUnavailable, false),
			Entry("when the status code is 504", http.StatusGatewayTimeout, serverErrorBody(http.StatusGatewayTimeout), nil, ToPtr(serverErrorBodyErrorMessage), http.StatusGatewayTimeout, false),
			Entry("when the status code is 505", http.StatusHTTPVersionNotSupported, serverErrorBody(http.StatusHTTPVersionNotSupported), nil, ToPtr(serverErrorBodyErrorMessage), http.StatusHTTPVersionNotSupported, false),
			Entry("when the status code is 506", http.StatusVariantAlsoNegotiates, serverErrorBody(http.StatusVariantAlsoNegotiates), nil, ToPtr(serverErrorBodyErrorMessage), http.StatusVariantAlsoNegotiates, false),
			// If the response body cannot be decoded, this may be due to a breaking change in the response format.
			// However, it could also be related to the server error. Therefore,
			//	- do not return an error (the decoding failure could be due to a transient error that caused a bad request format)
			//	- set the response body as the error on the response object
			//	- set the response object status code to the status code of the response
			//	- retry (not done)
			Entry("when the status code is 500 but the response body cannot be decoded", http.StatusInternalServerError, undecodableBody, nil, ToPtr(undecodableBodyErrorMessage), http.StatusInternalServerError, false),
			Entry("when the status code is 501 but the response body cannot be decoded", http.StatusNotImplemented, undecodableBody, nil, ToPtr(undecodableBodyErrorMessage), http.StatusNotImplemented, false),
			Entry("when the status code is 502 but the response body cannot be decoded", http.StatusBadGateway, undecodableBody, nil, ToPtr(undecodableBodyErrorMessage), http.StatusBadGateway, false),
			Entry("when the status code is 503 but the response body cannot be decoded", http.StatusServiceUnavailable, undecodableBody, nil, ToPtr(undecodableBodyErrorMessage), http.StatusServiceUnavailable, false),
		)
	})

	Context("when handling multiple-item responses", func() {
		It("will retry eligible items", func() {
			nCalls := 0

			client := &MockHttpClient{
				DoFunc: func(req *http.Request) (*http.Response, error) {
					var response *http.Response
					switch nCalls {
					case 0:
						resBody := MarshalAndStringify(skyflow.DetokenizeResponse{
							Response: []*skyflow.DetokenizeResponseObject{
								{
									// item 0: successful response
									Value:    "value0",
									HttpCode: ToPtr(http.StatusOK),
								},
								{
									// item 1: unretryable error
									Error:    ToPtr("error1"),
									HttpCode: ToPtr(http.StatusContinue),
								},
								{
									// item 2: retryable error
									Error:    ToPtr("error2"),
									HttpCode: ToPtr(http.StatusTooManyRequests),
								},
								{
									// item 3: unretryable error
									Error:    ToPtr("error3"),
									HttpCode: ToPtr(http.StatusMultipleChoices),
								},
								{
									// item 4: successful response
									Value:    "value4",
									HttpCode: ToPtr(http.StatusOK),
								},
								{
									// item 5: retryable error
									Error:    ToPtr("error5"),
									HttpCode: ToPtr(http.StatusInternalServerError),
								},
								{
									// item 6: unretryable error
									Error:    ToPtr("error6"),
									HttpCode: ToPtr(http.StatusBadRequest),
								},
							},
						})
						response = &http.Response{
							StatusCode: http.StatusMultiStatus,
							Body:       io.NopCloser(bytes.NewBufferString(resBody)),
						}
					case 1:
						resBody := MarshalAndStringify(skyflow.DetokenizeResponse{
							Response: []*skyflow.DetokenizeResponseObject{
								{
									// item 2: retryable error
									Error:    ToPtr("error2"),
									HttpCode: ToPtr(http.StatusGatewayTimeout),
								},
								{
									// item 5: successful response
									Value:    "value5",
									HttpCode: ToPtr(http.StatusOK),
								},
							},
						})
						response = &http.Response{
							StatusCode: http.StatusMultiStatus,
							Body:       io.NopCloser(bytes.NewBufferString(resBody)),
						}
					case 2:
						resBody := MarshalAndStringify(skyflow.DetokenizeResponse{
							Response: []*skyflow.DetokenizeResponseObject{
								{
									// item 2: successful response
									Value:    "value2",
									HttpCode: ToPtr(http.StatusOK),
								},
							},
						})
						response = &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewBufferString(resBody)),
						}
					default:
						panic(fmt.Sprintf("http doer should not be called %d times", nCalls))
					}
					nCalls++
					return response, nil
				},
			}

			flow, err := skyflow.NewDetokenizeFlow(
				client,
				1*time.Second,
				"vaultID",
				"https://vault-host",
				"bearer",
				[]string{"token1", "token2", "token3", "token4", "token5", "token6", "token7"},
			)
			Expect(err).To(BeNil())

			// After every call to Send, the flow should retry eligible items.

			// nCalls == 0
			err = flow.Send()
			Expect(err).To(BeNil())

			results := flow.Results()
			Expect(flow.IsDone()).To(BeFalse())
			Expect(results).To(HaveLen(7))

			Expect(results[0].Value).To(Equal("value0"))
			Expect(*results[0].HttpCode).To(Equal(http.StatusOK))

			Expect(*results[1].Error).To(Equal("error1"))
			Expect(*results[1].HttpCode).To(Equal(http.StatusContinue))

			Expect(*results[2].Error).To(Equal("error2"))
			Expect(*results[2].HttpCode).To(Equal(http.StatusTooManyRequests))

			Expect(*results[3].Error).To(Equal("error3"))
			Expect(*results[3].HttpCode).To(Equal(http.StatusMultipleChoices))

			Expect(results[4].Value).To(Equal("value4"))
			Expect(*results[4].HttpCode).To(Equal(http.StatusOK))

			Expect(*results[5].Error).To(Equal("error5"))
			Expect(*results[5].HttpCode).To(Equal(http.StatusInternalServerError))

			Expect(*results[6].Error).To(Equal("error6"))
			Expect(*results[6].HttpCode).To(Equal(http.StatusBadRequest))

			// nCalls == 1
			err = flow.Send()
			Expect(err).To(BeNil())

			results = flow.Results()
			Expect(flow.IsDone()).To(BeFalse())
			Expect(results).To(HaveLen(7))

			Expect(results[0].Value).To(Equal("value0"))
			Expect(*results[0].HttpCode).To(Equal(http.StatusOK))

			Expect(*results[1].Error).To(Equal("error1"))
			Expect(*results[1].HttpCode).To(Equal(http.StatusContinue))

			Expect(*results[2].Error).To(Equal("error2"))
			Expect(*results[2].HttpCode).To(Equal(http.StatusGatewayTimeout))

			Expect(*results[3].Error).To(Equal("error3"))
			Expect(*results[3].HttpCode).To(Equal(http.StatusMultipleChoices))

			Expect(results[4].Value).To(Equal("value4"))
			Expect(*results[4].HttpCode).To(Equal(http.StatusOK))

			Expect(results[5].Value).To(Equal("value5"))
			Expect(*results[5].HttpCode).To(Equal(http.StatusOK))

			Expect(*results[6].Error).To(Equal("error6"))
			Expect(*results[6].HttpCode).To(Equal(http.StatusBadRequest))

			// nCalls == 2
			err = flow.Send()
			Expect(err).To(BeNil())

			results = flow.Results()
			Expect(flow.IsDone()).To(BeTrue())
			Expect(results).To(HaveLen(7))

			Expect(results[0].Value).To(Equal("value0"))
			Expect(*results[0].HttpCode).To(Equal(http.StatusOK))

			Expect(*results[1].Error).To(Equal("error1"))
			Expect(*results[1].HttpCode).To(Equal(http.StatusContinue))

			Expect(results[2].Value).To(Equal("value2"))
			Expect(*results[2].HttpCode).To(Equal(http.StatusOK))

			Expect(*results[3].Error).To(Equal("error3"))
			Expect(*results[3].HttpCode).To(Equal(http.StatusMultipleChoices))

			Expect(results[4].Value).To(Equal("value4"))
			Expect(*results[4].HttpCode).To(Equal(http.StatusOK))

			Expect(results[5].Value).To(Equal("value5"))
			Expect(*results[5].HttpCode).To(Equal(http.StatusOK))

			Expect(*results[6].Error).To(Equal("error6"))
			Expect(*results[6].HttpCode).To(Equal(http.StatusBadRequest))
		})
	})
})
