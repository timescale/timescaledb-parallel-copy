package csvcopy

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
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
		if reportingPeriod <= 0 {
			return fmt.Errorf("reporting period must be greater than zero")
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
		if headerLineCount == 0 {
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

type WriteFS interface {
	CreateFile(name string) (io.WriteCloser, error)
}

// WithSkipFailedBatch sets a destination for the failed batches.
// When a batch fails to be inserted, instead of failing the entire operation,
// It will save the batch to the given location and proceed with the rest of the data.
// This way, the batch can be analysed later and reimported once the issue is fixed.
func WithSkipFailedBatch(destination WriteFS) Option {
	return func(c *Copier) error {
		c.failedBatchDestination = destination
		return nil
	}
}

type OSWriteFS struct {
	Root string
}

func (fs OSWriteFS) CreateFile(name string) (io.WriteCloser, error) {
	return os.Create(path.Join(fs.Root, name))
}

func WithSkipFailedBatchDir(dir string) Option {
	return func(c *Copier) error {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to ensure directory exists: %w", err)
		}
		c.failedBatchDestination = OSWriteFS{Root: dir}
		return nil
	}
}
