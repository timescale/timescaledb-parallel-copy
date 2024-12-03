package csvcopy

import (
	"errors"
	"strings"
)

type Option func(c *Copier) error

type Logger interface {
	Infof(msg string, args ...interface{})
}

type noopLogger struct{}

func (l *noopLogger) Infof(msg string, args ...interface{}) {}

// WithLogger sets the logger where the application will print debug messages
func WithLogger(logger Logger) Option {
	return func(c *Copier) error {
		c.logger = logger
		return nil
	}
}

// WithReportingFunction sets the function that will be called at
// reportingPeriod with information about the copy progress
func WithReportingFunction(f ReportFunc) Option {
	return func(c *Copier) error {
		c.reportingFunction = f
		return nil
	}
}

var HeaderInCopyOptionsError = errors.New("'HEADER' in copyOptions")

func WithCopyOptions(opt string) Option {
	return func(c *Copier) error {
		if strings.Contains(strings.ToUpper(opt), "HEADER") {
			return HeaderInCopyOptionsError
		}
		c.copyOptions = opt
		return nil
	}
}

func WithSplitCharacter(splitCharacter string) Option {
	return func(c *Copier) error {
		if len(splitCharacter) != 1 {
			return errors.New("split character must be a single-byte character")
		}
		c.splitCharacter = splitCharacter
		return nil
	}
}

func WithQuoteCharacter(quoteCharacter string) Option {
	return func(c *Copier) error {
		if len(quoteCharacter) != 1 {
			return errors.New("quote character must be a single-byte character")
		}

		c.quoteCharacter = quoteCharacter
		return nil
	}
}

func WithEscapeCharacter(escapeCharacter string) Option {
	return func(c *Copier) error {
		if len(escapeCharacter) > 1 {
			return errors.New("provided escape character must be a single-byte character")
		}

		c.escapeCharacter = escapeCharacter
		return nil
	}
}

func WithColumns(columns string) Option {
	return func(c *Copier) error {
		c.columns = columns
		return nil
	}
}

func WithSkipHeader(skipHeader bool) Option {
	return func(c *Copier) error {
		if c.skip != 0 {
			return errors.New("skip is already set. Use SkipHeader or SkipHeaderCount")
		}
		c.skip = 1
		return nil
	}
}

func WithSkipHeaderCount(headerLineCount int) Option {
	return func(c *Copier) error {
		if c.skip != 0 {
			return errors.New("skip is already set. Use SkipHeader or SkipHeaderCount")
		}
		if headerLineCount == 0 {
			return errors.New("header line count must be greater than zero")
		}
		c.skip = headerLineCount
		return nil
	}
}
