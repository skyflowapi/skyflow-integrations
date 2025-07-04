package logging

import (
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skyflowapi/skyflow-integrations/bigquery/common/messaging"
)

type Level uint32

const (
	FatalLevel Level = Level(log.FatalLevel)
	ErrorLevel Level = Level(log.ErrorLevel)
	WarnLevel  Level = Level(log.WarnLevel)
	InfoLevel  Level = Level(log.InfoLevel)
)

func (level Level) String() string {
	if b, err := level.Marshal(); err == nil {
		return string(b)
	} else {
		return "unknown"
	}
}

func (level Level) Marshal() ([]byte, error) {
	switch level {
	case InfoLevel:
		return []byte("info"), nil
	case WarnLevel:
		return []byte("warn"), nil
	case ErrorLevel:
		return []byte("error"), nil
	case FatalLevel:
		return []byte("fatal"), nil
	}
	return nil, fmt.Errorf("not a valid logging level %d", level)
}

// Loggers form a chain, each optionally applying formatting or adding context.
type Logger interface {
	WithContext(context ...string) Logger
	FormatAndAddContext(level Level, messages ...string) string
	SetLevel(level Level)

	Fatal(messages ...string)
	Error(messages ...string)
	Warn(messages ...string)
	Info(messages ...string)

	FatalNoContextNoFormat(message string)
	ErrorNoContextNoFormat(message string)
	WarnNoContextNoFormat(message string)
	InfoNoContextNoFormat(message string)
}

type RootLogger struct {
	wrapped *log.Logger
}

type messageFormatter struct{}

func (f *messageFormatter) Format(entry *log.Entry) ([]byte, error) {
	// All messages should have a newline at the end
	messageBytes := []byte(entry.Message)
	if len(messageBytes) <= 0 || messageBytes[len(messageBytes)-1] != '\n' {
		messageBytes = append(messageBytes, '\n')
	}
	return []byte(messageBytes), nil
}

func NewRootLogger() *RootLogger {
	wrapped := log.StandardLogger()
	wrapped.SetFormatter(&messageFormatter{})
	return &RootLogger{wrapped: wrapped}
}

type ContextualizedLogger struct {
	wrapped Logger
	context []string
}

func (l *RootLogger) WithContext(context ...string) Logger {
	return &ContextualizedLogger{
		wrapped: l,
		context: context,
	}
}

func (l *RootLogger) FormatAndAddContext(level Level, messages ...string) string {
	merged := append([]string{strings.ToUpper(level.String())}, messages...)
	return strings.Join(merged, " | ")
}

func (l *RootLogger) SetLevel(level Level) {
	l.wrapped.SetLevel(log.Level(level))
}

func (l *RootLogger) Fatal(messages ...string) {
	l.FatalNoContextNoFormat(l.FormatAndAddContext(FatalLevel, messages...))
}

func (l *RootLogger) Error(messages ...string) {
	l.ErrorNoContextNoFormat(l.FormatAndAddContext(ErrorLevel, messages...))
}

func (l *RootLogger) Warn(messages ...string) {
	l.WarnNoContextNoFormat(l.FormatAndAddContext(WarnLevel, messages...))
}

func (l *RootLogger) Info(messages ...string) {
	l.InfoNoContextNoFormat(l.FormatAndAddContext(InfoLevel, messages...))
}

func (l *RootLogger) FatalNoContextNoFormat(message string) {
	l.wrapped.Fatal(message)
}

func (l *RootLogger) ErrorNoContextNoFormat(message string) {
	l.wrapped.Error(message)
}

func (l *RootLogger) WarnNoContextNoFormat(message string) {
	l.wrapped.Warn(message)
}

func (l *RootLogger) InfoNoContextNoFormat(message string) {
	l.wrapped.Info(message)
}

func (l *ContextualizedLogger) WithContext(context ...string) Logger {
	return &ContextualizedLogger{
		wrapped: l,
		context: context,
	}
}

func (l *ContextualizedLogger) FormatAndAddContext(level Level, messages ...string) string {
	merged := make([]string, 0, len(l.context)+len(messages))
	merged = append(merged, l.context...)
	merged = append(merged, messages...)
	return l.wrapped.FormatAndAddContext(level, merged...)
}

func (l *ContextualizedLogger) SetLevel(level Level) {
	l.wrapped.SetLevel(level)
}

func (l *ContextualizedLogger) Fatal(messages ...string) {
	l.FatalNoContextNoFormat(l.FormatAndAddContext(FatalLevel, messages...))
}

func (l *ContextualizedLogger) Error(messages ...string) {
	l.ErrorNoContextNoFormat(l.FormatAndAddContext(ErrorLevel, messages...))
}

func (l *ContextualizedLogger) Warn(messages ...string) {
	l.WarnNoContextNoFormat(l.FormatAndAddContext(WarnLevel, messages...))
}

func (l *ContextualizedLogger) Info(messages ...string) {
	l.InfoNoContextNoFormat(l.FormatAndAddContext(InfoLevel, messages...))
}

func (l *ContextualizedLogger) FatalNoContextNoFormat(message string) {
	l.wrapped.FatalNoContextNoFormat(message)
}

func (l *ContextualizedLogger) ErrorNoContextNoFormat(message string) {
	l.wrapped.ErrorNoContextNoFormat(message)
}

func (l *ContextualizedLogger) WarnNoContextNoFormat(message string) {
	l.wrapped.WarnNoContextNoFormat(message)
}

func (l *ContextualizedLogger) InfoNoContextNoFormat(message string) {
	l.wrapped.InfoNoContextNoFormat(message)
}

var gcpLogLevelByLevel = map[Level]messaging.GCPLogLevel{
	FatalLevel: messaging.GCPLogLevelEmergency,
	ErrorLevel: messaging.GCPLogLevelError,
	WarnLevel:  messaging.GCPLogLevelWarning,
	InfoLevel:  messaging.GCPLogLevelInfo,
}

type GCPCloudRunStructuredLogger struct {
	TraceID string

	wrapped Logger
}

func (l *GCPCloudRunStructuredLogger) WithContext(context ...string) Logger {
	// The structured logger must be the last in the chain; contextualize the wrapped logger
	l.wrapped = l.wrapped.WithContext(context...)
	return l
}

func (l *GCPCloudRunStructuredLogger) FormatAndAddContext(level Level, messages ...string) string {
	wrappedFormatted := l.wrapped.FormatAndAddContext(level, messages...)
	wrappedFormatted = strings.TrimSpace(wrappedFormatted)
	entry := messaging.GCPCloudRunLogEntry{
		Message:  wrappedFormatted,
		Severity: string(gcpLogLevelByLevel[level]),
		Trace:    l.TraceID,
	}
	formattedBytes, err := json.Marshal(entry)
	if err != nil {
		// Defer to wrapped's format
		formattedBytes = []byte(wrappedFormatted)
	}
	return string(formattedBytes)
}

func (l *GCPCloudRunStructuredLogger) SetLevel(level Level) {
	l.wrapped.SetLevel(level)
}

func (l *GCPCloudRunStructuredLogger) Fatal(messages ...string) {
	l.FatalNoContextNoFormat(l.FormatAndAddContext(FatalLevel, messages...))
}

func (l *GCPCloudRunStructuredLogger) Error(messages ...string) {
	l.ErrorNoContextNoFormat(l.FormatAndAddContext(ErrorLevel, messages...))
}

func (l *GCPCloudRunStructuredLogger) Warn(messages ...string) {
	l.WarnNoContextNoFormat(l.FormatAndAddContext(WarnLevel, messages...))
}

func (l *GCPCloudRunStructuredLogger) Info(messages ...string) {
	l.InfoNoContextNoFormat(l.FormatAndAddContext(InfoLevel, messages...))
}

func (l *GCPCloudRunStructuredLogger) FatalNoContextNoFormat(message string) {
	l.wrapped.FatalNoContextNoFormat(message)
}

func (l *GCPCloudRunStructuredLogger) ErrorNoContextNoFormat(message string) {
	l.wrapped.ErrorNoContextNoFormat(message)
}

func (l *GCPCloudRunStructuredLogger) WarnNoContextNoFormat(message string) {
	l.wrapped.WarnNoContextNoFormat(message)
}

func (l *GCPCloudRunStructuredLogger) InfoNoContextNoFormat(message string) {
	l.wrapped.InfoNoContextNoFormat(message)
}

func NewGCPCloudRunStructuredLogger(traceID string, logger Logger) *GCPCloudRunStructuredLogger {
	return &GCPCloudRunStructuredLogger{
		wrapped: logger,
		TraceID: traceID,
	}
}

func ParseLevel(level string) (Level, error) {
	parsedLevel, err := log.ParseLevel(level)
	if err != nil {
		return 0, fmt.Errorf("not a valid logging level: %#v", level)
	}
	return Level(parsedLevel), nil
}
