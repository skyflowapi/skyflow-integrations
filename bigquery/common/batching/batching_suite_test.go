package batching_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBatching(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Batching Suite")
}
