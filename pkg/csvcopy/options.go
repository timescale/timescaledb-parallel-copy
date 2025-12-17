package csvcopy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
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
		c.Logger = logger
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
		if c.useFileHeaders == HeaderAutoColumnMapping || c.useFileHeaders == HeaderColumnMapping {
			return errors.New("column mapping is already set. Use only one of: WithColumns, WithColumnMapping, or WithAutoColumnMapping")
		}
		c.columns = columns
		return nil
	}
}

// WithSkipHeader is set, skips the first row of the csv file
func WithSkipHeader(skipHeader bool) Option {
	return func(c *Copier) error {
		if c.useFileHeaders != HeaderNone {
			return errors.New("header handling is already configured. Use only one of: WithSkipHeader, WithColumnMapping, or WithAutoColumnMapping")
		}
		c.useFileHeaders = HeaderSkip
		return nil
	}
}

// WithSkipHeaderCount sets the number of lines to skip at the beginning of the file
func WithSkipHeaderCount(headerLineCount int) Option {
	return func(c *Copier) error {
		if c.skip != 0 {
			return errors.New("skip is already set")
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

// WithQueueSize sets the size of the channel used to transfer batches from the producer to workers.
// The default queue size is 2 * workers.
func WithQueueSize(queueSize int) Option {
	return func(c *Copier) error {
		if queueSize <= 0 {
			return errors.New("queueSize must be greater than zero")
		}
		c.queueSize = queueSize
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

// WithBufferSize sets the buffer size
func WithBufferSize(bufferSize int) Option {
	return func(c *Copier) error {
		if bufferSize < 16 { // minimum buffer size on bufio.NewReaderSize
			return errors.New("buffer size must be greater than minimum buffer size (16)")
		}
		c.bufferSize = bufferSize
		return nil
	}
}

// WithBatchByteSize sets the max number of bytes to send in a batch
func WithBatchByteSize(batchByteSize int) Option {
	return func(c *Copier) error {
		if batchByteSize < 16 { // minimum buffer size on bufio.NewReaderSize
			return errors.New("batch byte size must be greater than minimum buffer size (16)")
		}
		c.batchByteSize = batchByteSize
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

// WithDirectCompress disable the use of Direct Compress
func WithUseDirectCompress(useDirectCompress bool) Option {
	return func(c *Copier) error {
		c.useDirectCompress = useDirectCompress
		return nil
	}
}

// WithClientSideSorting set the GUC that the client side data is pre-sorted
func WithHasClientSideSorting(hasClientSideSorting bool) Option {
	return func(c *Copier) error {
		// Can only be used in combination with direct compress
		if hasClientSideSorting && !c.useDirectCompress {
			return errors.New("Direct Compress can not be disabled in combination with enabled client side sorting.")
		}

		c.hasClientSideSorting = hasClientSideSorting
		return nil
	}
}

func NewErrContinue(err error) HandleBatchErrorResult {
	return HandleBatchErrorResult{
		Continue:     true,
		Err:          err,
		Handled:      false,
		InsertedRows: 0,
		SkippedRows:  0,
	}
}

func NewErrStop(err error) HandleBatchErrorResult {
	return HandleBatchErrorResult{
		Continue:     false,
		Err:          err,
		Handled:      false,
		InsertedRows: 0,
		SkippedRows:  0,
	}
}

type HandleBatchErrorResult struct {
	// Continue if true, The code will continue processing new batches. Otherwise, it will stop.
	Continue bool
	// Handled if true, It means the error was correctly handled and the resulting batch will be marked as completed
	Handled bool
	// Rows number of rows successfully processed by the error handler.
	// This ensures metrics stay up to date when error handlers can process the rows.
	InsertedRows int64
	// Rows found but skipped due to a known reason
	SkippedRows int64
	// Err is the error that was returned by the error handler. It may just return the original error to act as a middleware or a new error to indicate failure reason.
	// The transaction will fail with this error if it is not a temporary error.
	Err error
}

func (err HandleBatchErrorResult) Error() string {
	return fmt.Sprintf("continue: %t, %s", err.Continue, err.Err)
}

func (err HandleBatchErrorResult) Unwrap() error {
	return err.Err
}

// BatchErrorHandler is how batch errors are handled
// It has the batch data so it can be inspected
// The error has the failure reason
// If the error is not handled properly, returning an error will stop the workers
// If ErrContinue is returned, the batch will be marked as failed but continue processing
// if ErrStop is returned, the processing will stop
type BatchErrorHandler func(
	ctx context.Context,
	// c is the copier that is being used to handle the error
	c *Copier,
	// db is the database connection running a transaction that is being used to handle the error
	// It connects to the target database
	// The error handler is not the owner of the transaction, so it must not commit or rollback it.
	db *sqlx.Conn,
	// batch is the batch that has the error
	batch Batch,
	// err is the error that was returned the copy operation.
	err error,
) HandleBatchErrorResult

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
// It is safe to run concurrently for the same ID.
//
// Note: Using the same import id has the following expectation
// - The input file will be the same
// - The batch size will be the same
//
// If those expectation are not met, the behaviour of the tool is not specified and
// will probably end up inserting duplicate records.
func WithImportID(id string) Option {
	return func(c *Copier) error {
		if id == "" {
			return errors.New("importID can't be empty")
		}
		c.importID = id
		return nil
	}
}

// WithIdempotencyWindow sets the idempotency window for the import operation
// Records older than the window will be deleted from the transaction table
// Default is 4 weeks
func WithIdempotencyWindow(window time.Duration) Option {
	return func(c *Copier) error {
		if window < 0 {
			return errors.New("idempotency window must be greater than zero")
		}
		c.idempotencyWindow = window
		return nil
	}
}

// WithColumnMapping sets the column mapping from CSV header names to database column names
// Each ColumnMapping specifies CSVColumnName and DatabaseColumnName
// This option automatically enables header skipping (sets skip to 1)
func WithColumnMapping(mappings []ColumnMapping) Option {
	return func(c *Copier) error {
		if mappings == nil {
			return errors.New("column mapping cannot be nil")
		}
		if c.useFileHeaders != HeaderNone {
			return errors.New("header handling is already configured. Use only one of: WithSkipHeader, WithColumnMapping, or WithAutoColumnMapping")
		}
		if c.columns != "" {
			return errors.New("columns are already set. Use only one of: WithColumns, WithColumnMapping, or WithAutoColumnMapping")
		}
		for i, mapping := range mappings {
			if mapping.CSVColumnName == "" {
				return fmt.Errorf("column mapping at index %d has empty CSVColumnName", i)
			}
			if mapping.DatabaseColumnName == "" {
				return fmt.Errorf("column mapping at index %d has empty DatabaseColumnName", i)
			}
		}
		c.columnMapping = mappings
		c.useFileHeaders = HeaderColumnMapping
		return nil
	}
}

// WithAutoColumnMapping enables automatic column mapping where CSV header names
// are used as database column names (1:1 mapping)
// This option automatically enables header skipping (sets skip to 1)
func WithAutoColumnMapping() Option {
	return func(c *Copier) error {
		if c.useFileHeaders != HeaderNone {
			return errors.New("header handling is already configured. Use only one of: WithSkipHeader, WithColumnMapping, or WithAutoColumnMapping")
		}
		if c.columns != "" {
			return errors.New("columns are already set. Use only one of: WithColumns, WithColumnMapping, or WithAutoColumnMapping")
		}
		c.useFileHeaders = HeaderAutoColumnMapping
		return nil
	}
}
