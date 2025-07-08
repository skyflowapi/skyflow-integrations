// Copyright (c) 2025 Skyflow, Inc.

package skyflow_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging/skyflow"
)

var _ = Describe("Refreshing a bearer token", func() {
	Context("when the bearer token is expired", func() {
		It("will refresh the bearer token", func() {
			bearer := "expired"
			generateBearer := func() (string, error) {
				return "new-bearer", nil
			}
			isBearerExpired := func(bearer string) bool {
				return true
			}
			bearer, err := skyflow.GenerateBearerIfExpired(
				bearer,
				generateBearer,
				isBearerExpired,
			)

			Expect(err).To(BeNil())
			Expect(bearer).To(Equal("new-bearer"))
		})
	})

	Context("when the bearer token is not expired", func() {
		It("will not refresh the bearer token", func() {
			bearer := "not-expired"
			generateBearer := func() (string, error) {
				return "new-bearer", nil
			}
			isBearerExpired := func(bearer string) bool {
				return false
			}
			bearer, err := skyflow.GenerateBearerIfExpired(
				bearer,
				generateBearer,
				isBearerExpired,
			)

			Expect(err).To(BeNil())
			Expect(bearer).To(Equal("not-expired"))
		})
	})

	Context("when the token is expired and the refresh fails", func() {
		It("will return an error", func() {
			bearer := "expired"
			generateBearer := func() (string, error) {
				return "", errors.New("refresh failed")
			}
			isBearerExpired := func(bearer string) bool {
				return true
			}
			bearer, err := skyflow.GenerateBearerIfExpired(
				bearer,
				generateBearer,
				isBearerExpired,
			)

			Expect(err).To(MatchError("failed to authenticate with the Skyflow API: refresh failed"))
			Expect(bearer).To(Equal(""))
		})
	})
})
