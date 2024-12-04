package csvcopy

import (
	"errors"
	"fmt"
	"strings"
	"time"
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

func WithReportingPeriod(reportingPeriod time.Duration) Option {
	return func(c *Copier) error {
		if reportingPeriod <= 0 {
			return fmt.Errorf("reporting period must be greater than zero")
		}
		c.reportingPeriod = reportingPeriod
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
		if len(splitCharacter) > 1 {
			return errors.New("split character must be a single-byte character")
		}
		c.splitCharacter = splitCharacter
		return nil
	}
}

func WithQuoteCharacter(quoteCharacter string) Option {
	return func(c *Copier) error {
		if len(quoteCharacter) > 1 {
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

func WithWorkers(workers int) Option {
	return func(c *Copier) error {
		if workers <= 0 {
			return errors.New("workers must be greater than zero")
		}
		c.workers = workers
		return nil
	}
}

func WithLimit(limit int64) Option {
	return func(c *Copier) error {
		if limit < 0 {
			return errors.New("limit must be greater than zero")
		}
		c.limit = limit
		return nil
	}
}

func WithBatchSize(batchSize int) Option {
	return func(c *Copier) error {
		if batchSize < 0 {
			return errors.New("batch size must be greater than zero")
		}
		c.batchSize = batchSize
		return nil
	}
}

func WithLogBatches(logBatches bool) Option {
	return func(c *Copier) error {
		c.logBatches = logBatches
		return nil
	}
}

func WithVerbose(verbose bool) Option {
	return func(c *Copier) error {
		c.verbose = verbose
		return nil
	}
}

func WithSchemaName(schema string) Option {
	return func(c *Copier) error {
		c.schemaName = schema
		return nil
	}
}
