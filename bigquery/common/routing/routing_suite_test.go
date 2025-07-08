// Copyright (c) 2025 Skyflow, Inc.

package routing_test

import (
	"testing"

	"github.com/gin-gonic/gin"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRouting(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Routing Suite")
}

var _ = BeforeSuite(func() {
	gin.SetMode(gin.TestMode)
})
