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
// ReportingPeriod with information about the copy progress
func WithReportingFunction(f ReportFunc) Option {
	return func(c *Copier) error {
		if c.reportingPeriod == 0 {
			return fmt.Errorf("reporting period must be set before the reporting function")
		}
		c.reportingFunction = f
		return nil
	}
}

// WithReportingPeriod sets how often the reporting function will be called.
func WithReportingPeriod(reportingPeriod time.Duration) Option {
	return func(c *Copier) error {
		if reportingPeriod < 0 {
			return fmt.Errorf("reporting period must be equal or greater than zero")
		}
		c.reportingPeriod = reportingPeriod
		return nil
	}
}

var HeaderInCopyOptionsError = errors.New("'HEADER' in copyOptions")

// WithCopyOptions appends the COPY options for the COPY operation.
// By default is 'CSV'
func WithCopyOptions(opt string) Option {
	return func(c *Copier) error {
		if strings.Contains(strings.ToUpper(opt), "HEADER") {
			return HeaderInCopyOptionsError
		}
		c.copyOptions = opt
		return nil
	}
}

// WithSplitCharacter sets the COPY option DELIMITER
func WithSplitCharacter(splitCharacter string) Option {
	return func(c *Copier) error {
		if len(splitCharacter) > 1 {
			return errors.New("split character must be a single-byte character")
		}
		c.splitCharacter = splitCharacter
		return nil
	}
}

// WithQuoteCharacter sets the COPY option QUOTE
func WithQuoteCharacter(quoteCharacter string) Option {
	return func(c *Copier) error {
		if len(quoteCharacter) > 1 {
			return errors.New("quote character must be a single-byte character")
		}

		c.quoteCharacter = quoteCharacter
		return nil
	}
}

// WithEscapeCharacter sets the COPY option ESCAPE
func WithEscapeCharacter(escapeCharacter string) Option {
	return func(c *Copier) error {
		if len(escapeCharacter) > 1 {
			return errors.New("provided escape character must be a single-byte character")
		}

		c.escapeCharacter = escapeCharacter
		return nil
	}
}

// WithColumns accepts a list of comma separated values for the csv columns
func WithColumns(columns string) Option {
	return func(c *Copier) error {
		c.columns = columns
		return nil
	}
}

// WithSkipHeader is set, skips the first row of the csv file
func WithSkipHeader(skipHeader bool) Option {
	return func(c *Copier) error {
		if c.skip != 0 {
			return errors.New("skip is already set. Use SkipHeader or SkipHeaderCount")
		}
		c.skip = 1
		return nil
	}
}

// WithSkipHeaderCount sets the number of lines to skip at the beginning of the file
func WithSkipHeaderCount(headerLineCount int) Option {
	return func(c *Copier) error {
		if c.skip != 0 {
			return errors.New("skip is already set. Use SkipHeader or SkipHeaderCount")
		}
		if headerLineCount <= 0 {
			return errors.New("header line count must be greater than zero")
		}
		c.skip = headerLineCount
		return nil
	}
}

// WithWorkers sets the number of workers to use while processing the file
func WithWorkers(workers int) Option {
	return func(c *Copier) error {
		if workers <= 0 {
			return errors.New("workers must be greater than zero")
		}
		c.workers = workers
		return nil
	}
}

// WithLimit limits the number of imported rows
func WithLimit(limit int64) Option {
	return func(c *Copier) error {
		if limit < 0 {
			return errors.New("limit must be greater than zero")
		}
		c.limit = limit
		return nil
	}
}

// WithBatchSize sets the rows processed on each batch
func WithBatchSize(batchSize int) Option {
	return func(c *Copier) error {
		if batchSize < 0 {
			return errors.New("batch size must be greater than zero")
		}
		c.batchSize = batchSize
		return nil
	}
}

// WithLogBatches prints a line for every processed batch
func WithLogBatches(logBatches bool) Option {
	return func(c *Copier) error {
		c.logBatches = logBatches
		return nil
	}
}

// WithVerbose enables logging
func WithVerbose(verbose bool) Option {
	return func(c *Copier) error {
		c.verbose = verbose
		return nil
	}
}

// WithSchemaName sets the schema name
func WithSchemaName(schema string) Option {
	return func(c *Copier) error {
		c.schemaName = schema
		return nil
	}
}

// BatchErrorHandler is how batch errors are handled
// It has the batch data so it can be inspected
// The error has the failure reason
// If the error is not handled properly, returning an error will stop the workers
type BatchErrorHandler func(batch Batch, err error) error

// WithBatchErrorHandler specifies which fail handler implementation to use
func WithBatchErrorHandler(handler BatchErrorHandler) Option {
	return func(c *Copier) error {
		c.failHandler = handler
		return nil
	}
}

// WithImportID specifies the ID for the import operation to guarantee idempotency
// The tool will keep track of every batch to insert in the database and update the
// status according to the result of the operation.
// This information can be used to recover in case of an abrupt stop or just to resume
// the operation after a graceful stop before the entire file was processed
//
// Usage: For every unique file that has to be inserted in the database, generate an
// unique identifier. As long as configuration remains the same,
// It is safe to run the same command multiple times.
// It is NOT safe to run concurrently for the same ID.
//
// Note: Using the same import id has the following expectation
// - The input file will be the same
// - The batch size will be the same
//
// If those expectation are not met, the behaviour of the tool is not specified and
// will provably end up inserting duplicate records.
func WithImportID(id string) Option {
	return func(c *Copier) error {
		if id == "" {
			return errors.New("importID can't be empty")
		}
		c.importID = id
		return nil
	}
}
