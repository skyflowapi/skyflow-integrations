// Copyright (c) 2025 Skyflow, Inc.

package logging_test

import (
	"bytes"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	log "github.com/sirupsen/logrus"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/logging"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
	. "github.com/skyflowapi/skyflow-integrations/bigquery/common/test"
)

var _ = Describe("Stringifying the logging level", func() {
	DescribeTable(
		"stringifying valid levels",
		func(level logging.Level, expected string) {
			Expect(level.String()).To(Equal(expected))
		},
		Entry("info", logging.InfoLevel, "info"),
		Entry("warn", logging.WarnLevel, "warn"),
		Entry("error", logging.ErrorLevel, "error"),
		Entry("fatal", logging.FatalLevel, "fatal"),
	)

	It("should return unknown for an invalid level", func() {
		Expect(logging.Level(100).String()).To(Equal("unknown"))
	})
})

var _ = Describe("Marshalling the logging level", func() {
	DescribeTable(
		"marshalling the logging level",
		func(level logging.Level, expected []byte) {
			levelBytes, err := level.Marshal()
			Expect(err).To(BeNil())
			Expect(levelBytes).To(Equal(expected))
		},
		Entry("info", logging.InfoLevel, []byte("info")),
		Entry("warn", logging.WarnLevel, []byte("warn")),
		Entry("error", logging.ErrorLevel, []byte("error")),
		Entry("fatal", logging.FatalLevel, []byte("fatal")),
	)

	It("should return an error for an invalid level", func() {
		levelBytes, err := logging.Level(100).Marshal()
		Expect(levelBytes).To(BeNil())
		Expect(err).To(MatchError("not a valid logging level 100"))
	})
})

var _ = Describe("RootLogger", func() {
	var (
		buf      bytes.Buffer
		logger   *logging.RootLogger
		exitCode int
	)

	BeforeEach(func() {
		buf = bytes.Buffer{}
		wrapped := log.StandardLogger()
		wrapped.SetOutput(&buf)
		wrapped.ExitFunc = func(code int) {
			exitCode = code
		}
		logger = logging.NewRootLogger()
		logger.SetLevel(logging.InfoLevel)
	})

	It("should have a newline at the end of the message", func() {
		logger.Info("test")
		Expect(buf.String()).To(HaveSuffix("\n"))
	})

	DescribeTable(
		"non-fatal message is formatted correctly",
		func(doer func(), expected string) {
			doer()
			Expect(buf.String()).To(Equal(expected))
		},
		Entry("info", func() { logger.Info("test") }, "INFO | test\n"),
		Entry("warn", func() { logger.Warn("test") }, "WARN | test\n"),
		Entry("error", func() { logger.Error("test") }, "ERROR | test\n"),
	)

	It("should exit the process when a fatal message is logged", func() {
		logger.Fatal("test")
		Expect(buf.String()).To(Equal("FATAL | test\n"))
		Expect(exitCode).To(Equal(1))
	})

	DescribeTable(
		"setting the logging level",
		func(
			level logging.Level,
			showFatal bool,
			showError bool,
			showWarn bool,
			showInfo bool,
		) {
			logger.SetLevel(level)

			handler := []func(messages ...string){
				logger.Fatal,
				logger.Error,
				logger.Warn,
				logger.Info,
			}
			show := []bool{showFatal, showError, showWarn, showInfo}
			prefix := []string{"FATAL", "ERROR", "WARN", "INFO"}

			for i := range show {
				if show[i] {
					handler[i]("test")
					Expect(buf.String()).To(Equal(fmt.Sprintf("%s | test\n", prefix[i])))
					buf.Reset()
				} else {
					Expect(buf.Len()).To(BeZero())
				}
			}
		},
		Entry("info", logging.InfoLevel, true, true, true, true),
		Entry("warn", logging.WarnLevel, true, true, true, false),
		Entry("error", logging.ErrorLevel, true, true, false, false),
		Entry("fatal", logging.FatalLevel, true, false, false, false),
	)

	It("should be contextualizeable", func() {
		contextualizedAny := logger.WithContext("fake", "context")
		contextualized, ok := contextualizedAny.(*logging.ContextualizedLogger)
		Expect(ok).To(BeTrue())
		Expect(contextualized).ToNot(BeNil())
	})

	It("should be able to format messages", func() {
		formatted := logger.FormatAndAddContext(logging.InfoLevel, "test")
		Expect(formatted).To(Equal("INFO | test"))
	})
})

var _ = Describe("ContextualizedLogger", func() {
	var (
		buf      bytes.Buffer
		logger   logging.Logger
		exitCode int
		context  = []string{"fake", "context"}
	)

	BeforeEach(func() {
		buf = bytes.Buffer{}
		wrapped := log.StandardLogger()
		wrapped.SetOutput(&buf)
		wrapped.ExitFunc = func(code int) {
			exitCode = code
		}
		rootlogger := logging.NewRootLogger()
		rootlogger.SetLevel(logging.InfoLevel)
		logger = rootlogger.WithContext(context...)
	})

	It("should have a newline at the end of the message", func() {
		logger.Info("test")
		Expect(buf.String()).To(HaveSuffix("\n"))
	})

	DescribeTable(
		"non-fatal message is formatted correctly",
		func(doer func(), expected string) {
			doer()
			Expect(buf.String()).To(Equal(expected))
		},
		Entry("info", func() { logger.Info("test") }, "INFO | fake | context | test\n"),
		Entry("warn", func() { logger.Warn("test") }, "WARN | fake | context | test\n"),
		Entry("error", func() { logger.Error("test") }, "ERROR | fake | context | test\n"),
	)

	It("should exit the process when a fatal message is logged", func() {
		logger.Fatal("test")
		Expect(buf.String()).To(Equal("FATAL | fake | context | test\n"))
		Expect(exitCode).To(Equal(1))
	})

	DescribeTable(
		"setting the logging level",
		func(
			level logging.Level,
			showFatal bool,
			showError bool,
			showWarn bool,
			showInfo bool,
		) {
			logger.SetLevel(level)

			handler := []func(messages ...string){
				logger.Fatal,
				logger.Error,
				logger.Warn,
				logger.Info,
			}
			show := []bool{showFatal, showError, showWarn, showInfo}
			prefix := []string{"FATAL", "ERROR", "WARN", "INFO"}

			for i := range show {
				if show[i] {
					handler[i]("test")
					Expect(buf.String()).To(Equal(fmt.Sprintf("%s | fake | context | test\n", prefix[i])))
					buf.Reset()
				} else {
					Expect(buf.Len()).To(BeZero())
				}
			}
		},
		Entry("info", logging.InfoLevel, true, true, true, true),
		Entry("warn", logging.WarnLevel, true, true, true, false),
		Entry("error", logging.ErrorLevel, true, true, false, false),
		Entry("fatal", logging.FatalLevel, true, false, false, false),
	)

	It("should be contextualizeable", func() {
		contextualizedAny := logger.WithContext("extra", "context")
		contextualized, ok := contextualizedAny.(*logging.ContextualizedLogger)
		Expect(ok).To(BeTrue())
		formatted := contextualized.FormatAndAddContext(logging.InfoLevel, "test")
		Expect(formatted).To(Equal("INFO | fake | context | extra | context | test"))
	})

	It("should be able to format messages", func() {
		formatted := logger.FormatAndAddContext(logging.InfoLevel, "test")
		Expect(formatted).To(Equal("INFO | fake | context | test"))
	})
})

var _ = Describe("GCPCloudRunStructuredLogger", func() {
	var (
		buf      bytes.Buffer
		logger   logging.Logger
		exitCode int
		context  = []string{"fake", "context"}
		traceID  = "6C26BC42CF33A48D29746CAF746FAF7B"
	)

	BeforeEach(func() {
		buf = bytes.Buffer{}
		wrapped := log.StandardLogger()
		wrapped.SetOutput(&buf)
		wrapped.ExitFunc = func(code int) {
			exitCode = code
		}
		rootlogger := logging.NewRootLogger()
		rootlogger.SetLevel(logging.InfoLevel)
		logger = rootlogger.WithContext(context...)
		logger = logging.NewGCPCloudRunStructuredLogger(traceID, logger)
	})

	It("should have a newline at the end of the message", func() {
		logger.Info("test")
		Expect(buf.String()).To(HaveSuffix("\n"))
	})

	DescribeTable(
		"non-fatal message is formatted correctly",
		func(doer func(), expected string) {
			doer()
			Expect(buf.String()).To(Equal(expected + "\n"))
		},
		Entry("info", func() { logger.Info("test") }, MarshalAndStringify(messaging.GCPCloudRunLogEntry{
			Message:  "INFO | fake | context | test",
			Severity: "INFO",
			Trace:    traceID,
		})),
		Entry("warn", func() { logger.Warn("test") }, MarshalAndStringify(messaging.GCPCloudRunLogEntry{
			Message:  "WARN | fake | context | test",
			Severity: "WARNING",
			Trace:    traceID,
		})),
		Entry("error", func() { logger.Error("test") }, MarshalAndStringify(messaging.GCPCloudRunLogEntry{
			Message:  "ERROR | fake | context | test",
			Severity: "ERROR",
			Trace:    traceID,
		})),
	)

	It("should exit the process when a fatal message is logged", func() {
		logger.Fatal("test")
		Expect(buf.String()).To(Equal(MarshalAndStringify(messaging.GCPCloudRunLogEntry{
			Message:  "FATAL | fake | context | test",
			Severity: "EMERGENCY",
			Trace:    traceID,
		}) + "\n"))
		Expect(exitCode).To(Equal(1))
	})

	DescribeTable(
		"setting the logging level",
		func(
			level logging.Level,
			showFatal bool,
			showError bool,
			showWarn bool,
			showInfo bool,
		) {
			logger.SetLevel(level)

			handler := []func(messages ...string){
				logger.Fatal,
				logger.Error,
				logger.Warn,
				logger.Info,
			}
			show := []bool{showFatal, showError, showWarn, showInfo}
			prefix := []string{"FATAL", "ERROR", "WARN", "INFO"}
			severity := []string{"EMERGENCY", "ERROR", "WARNING", "INFO"}

			for i := range show {
				if show[i] {
					handler[i]("test")
					Expect(buf.String()).To(Equal(MarshalAndStringify(messaging.GCPCloudRunLogEntry{
						Message:  fmt.Sprintf("%s | fake | context | test", prefix[i]),
						Severity: severity[i],
						Trace:    traceID,
					}) + "\n"))
					buf.Reset()
				} else {
					Expect(buf.Len()).To(BeZero())
				}
			}
		},
		Entry("info", logging.InfoLevel, true, true, true, true),
		Entry("warn", logging.WarnLevel, true, true, true, false),
		Entry("error", logging.ErrorLevel, true, true, false, false),
		Entry("fatal", logging.FatalLevel, true, false, false, false),
	)

	It("should be contextualizeable", func() {
		contextualizedAny := logger.WithContext("extra", "context")
		contextualized, ok := contextualizedAny.(*logging.GCPCloudRunStructuredLogger)
		Expect(ok).To(BeTrue())
		formatted := contextualized.FormatAndAddContext(logging.InfoLevel, "test")
		Expect(formatted).To(Equal(MarshalAndStringify(messaging.GCPCloudRunLogEntry{
			Message:  "INFO | fake | context | extra | context | test",
			Severity: "INFO",
			Trace:    traceID,
		})))
	})

	It("should be able to format messages", func() {
		formatted := logger.FormatAndAddContext(logging.InfoLevel, "test")
		Expect(formatted).To(Equal(MarshalAndStringify(messaging.GCPCloudRunLogEntry{
			Message:  "INFO | fake | context | test",
			Severity: "INFO",
			Trace:    traceID,
		})))
	})
})

var _ = Describe("Parsing the logging level", func() {
	DescribeTable(
		"parsing the logging level",
		func(level string, expected logging.Level) {
			parsed, err := logging.ParseLevel(level)
			Expect(err).To(BeNil())
			Expect(parsed).To(Equal(expected))
		},
		Entry("info", "info", logging.InfoLevel),
		Entry("warn", "warn", logging.WarnLevel),
		Entry("error", "error", logging.ErrorLevel),
		Entry("fatal", "fatal", logging.FatalLevel),
	)

	It("should return an error for an invalid level", func() {
		parsed, err := logging.ParseLevel("invalid")
		Expect(parsed).To(BeZero())
		Expect(err).To(MatchError("not a valid logging level: \"invalid\""))
	})
})
