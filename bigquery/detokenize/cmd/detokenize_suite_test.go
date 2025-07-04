package main_test

import (
	"testing"

	"github.com/gin-gonic/gin"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDetokenize(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Detokenize Suite")
}

var _ = BeforeSuite(func() {
	gin.SetMode(gin.TestMode)
})
