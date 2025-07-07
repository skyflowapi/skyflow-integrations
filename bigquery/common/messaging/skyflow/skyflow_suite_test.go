// Copyright (c) 2025 Skyflow, Inc.

package skyflow_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSkyflow(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Skyflow Suite")
}
