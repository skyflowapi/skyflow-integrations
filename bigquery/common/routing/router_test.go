package routing_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gin-gonic/gin"

	"github.com/skyflowapi/skyflow-integrations/bigquery/common/routing"
)

var _ = Describe("Creating a router", func() {
	var router *gin.Engine

	BeforeEach(func() {
		var err error
		router, err = routing.CreateRouter()
		Expect(err).To(BeNil())
	})

	It("should create a router with the correct mode", func() {
		Expect(gin.Mode()).To(Equal(gin.ReleaseMode))
	})

	It("should create a router with the recovery middleware", func() {
		// contain element of type gin.Recovery
		Expect(router.Handlers).To(ContainElement(BeAssignableToTypeOf(gin.Recovery())))
	})
})
