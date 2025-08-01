package csvcopy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

const TAB_CHAR_STR = "\\t"

type Result struct {
	// InsertedRows is the number of rows inserted into the database by this copier instance
	InsertedRows int64
	// SkippedRows is the number of rows skipped because they were already processed
	SkippedRows int64
	// TotalRows is the number of rows read from source
	// rows may be skipped if already processed so it may differ from rows inserted
	TotalRows int64
	Duration  time.Duration
	RowRate   float64
}

type Copier struct {
	connString string
	tableName  string

	copyOptions string

	schemaName        string
	logger            Logger
	splitCharacter    string
	quoteCharacter    string
	escapeCharacter   string
	columns           string
	workers           int
	limit             int64
	bufferSize        int
	batchByteSize     int
	batchSize         int
	logBatches        bool
	reportingPeriod   time.Duration
	reportingFunction ReportFunc
	verbose           bool
	skip              int
	importID          string
	idempotencyWindow time.Duration

	// Rows that are inserted in the database by this copier instance
	insertedRows int64
	// Rows that are skipped because they were already processed
	skippedRows int64
	// Total rows read from the source
	totalRows int64

	failHandler BatchErrorHandler
}

func NewCopier(
	connString string,
	tableName string,
	options ...Option,
) (*Copier, error) {
	copier := &Copier{
		connString: connString,
		tableName:  tableName,

		// Defaults
		schemaName:        "public",
		logger:            &noopLogger{},
		copyOptions:       "CSV",
		splitCharacter:    ",",
		quoteCharacter:    "",
		escapeCharacter:   "",
		columns:           "",
		workers:           1,
		limit:             0,
		bufferSize:        10 * 1024 * 1024,
		batchByteSize:     50 * 1024 * 1024,
		batchSize:         5000,
		logBatches:        false,
		reportingPeriod:   0,
		verbose:           false,
		skip:              0,
		importID:          "",
		idempotencyWindow: 28 * 24 * time.Hour, // 4 weeks
	}

	for _, o := range options {
		err := o(copier)
		if err != nil {
			return nil, fmt.Errorf("failed to execute option %T: %w", o, err)
		}
	}

	if copier.skip > 0 && copier.verbose {
		copier.logger.Infof("Skipping the first %d lines of the input.", copier.skip)
	}

	if copier.reportingFunction == nil {
		copier.reportingFunction = DefaultReportFunc(copier.logger)
	}

	return copier, nil
}

func (c *Copier) Truncate() (err error) {
	dbx, err := connect(c.connString)
	if err != nil {
		return fmt.Errorf("failed to connect to the database: %w", err)
	}
	defer func() {
		err = dbx.Close()
	}()
	_, err = dbx.Exec(fmt.Sprintf("TRUNCATE %s", c.getFullTableName()))
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	return err
}

func (c *Copier) Copy(ctx context.Context, reader io.Reader) (Result, error) {

	if c.HasImportID() {
		if err := ensureTransactionTable(ctx, c.connString); err != nil {
			return Result{}, fmt.Errorf("failed to ensure transaction table, %w", err)
		}
		c.logger.Infof("Cleaning old transactions older than %s", c.idempotencyWindow)
		if err := cleanOldTransactions(ctx, c.connString, c.idempotencyWindow); err != nil {
			return Result{}, fmt.Errorf("failed to clean old transactions, %w", err)
		}
	}

	var workerWg sync.WaitGroup
	batchChan := make(chan Batch, c.workers*2)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, c.workers+1)

	// Generate COPY workers
	for i := 0; i < c.workers; i++ {
		workerWg.Add(1)
		go func(i int) {
			defer workerWg.Done()
			err := c.processBatches(ctx, batchChan)
			if err != nil {
				errCh <- err
				cancel()
			}
			c.logger.Infof("stop worker %d", i)
		}(i)

	}

	var supportWg sync.WaitGroup
	supportCtx, cancelSupportCtx := context.WithCancel(ctx)
	defer cancelSupportCtx()
	// Reporting thread
	if c.reportingPeriod > (0 * time.Second) {
		c.logger.Infof("There will be reports every %s", c.reportingPeriod.String())
		supportWg.Add(1)
		go func() {
			defer supportWg.Done()
			c.report(supportCtx)
		}()
	}

	opts := scanOptions{
		Size:           c.batchSize,
		Skip:           c.skip,
		Limit:          c.limit,
		BufferByteSize: c.bufferSize,
		BatchByteSize:  c.batchByteSize,
		ImportID:       c.importID,
	}

	if c.quoteCharacter != "" {
		// we already verified the length
		opts.Quote = c.quoteCharacter[0]
	}
	if c.escapeCharacter != "" {
		// we already verified the length
		opts.Escape = c.escapeCharacter[0]
	}

	start := time.Now()
	workerWg.Add(1)
	go func() {
		defer workerWg.Done()
		if err := scan(ctx, reader, batchChan, opts); err != nil {
			errCh <- fmt.Errorf("failed reading input: %w", err)
			cancel()
		}
		close(batchChan)
		c.logger.Infof("stop scan")
	}()
	workerWg.Wait()

	cancelSupportCtx()
	supportWg.Wait()

	close(errCh)
	// We are only interested on the first error message since all other errors
	// must probably are related to the context being canceled.
	err := <-errCh

	end := time.Now()
	took := end.Sub(start)

	insertedRows := c.GetInsertedRows()
	totalRows := c.GetTotalRows()
	skippedRows := c.GetSkippedRows()
	rowRate := float64(insertedRows) / float64(took.Seconds())

	result := Result{
		InsertedRows: insertedRows,
		TotalRows:    totalRows,
		SkippedRows:  skippedRows,
		Duration:     took,
		RowRate:      rowRate,
	}

	if err != nil {
		return result, err
	}
	return result, nil
}

type ErrAtRow struct {
	Err error
	// Row is the row reported by PgError
	// The value is relative to the location
	Row           int
	BatchLocation Location
}

// RowAtLocation returns the row number taking into account the batch location
// so the number matches the original file
// The row 0 is the first row of the file
func (err *ErrAtRow) RowAtLocation() int {
	if err.Row == -1 {
		return -1
	}
	return err.Row + int(err.BatchLocation.StartRow)
}

func ExtractRowFrom(pgerr *pgconn.PgError) int {
	// Example of Where field
	// "COPY metrics, line 1, column value: \"hello\""
	match := regexp.MustCompile(`line (\d+)`).FindStringSubmatch(pgerr.Where)
	if len(match) != 2 {
		return -1
	}

	line, err := strconv.Atoi(match[1])
	if err != nil {
		return -1
	}

	return line - 1
}

func (e ErrAtRow) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("at row %d, error %s", e.RowAtLocation(), e.Err.Error())
	}
	return fmt.Sprintf("error at row %d", e.RowAtLocation())
}

func (e ErrAtRow) Unwrap() error {
	return e.Err
}

// processBatches reads batches from channel c and copies them to the target
// server while tracking stats on the write.
func (c *Copier) processBatches(ctx context.Context, ch chan Batch) (err error) {
	dbx, err := connect(c.connString)
	if err != nil {
		return err
	}
	defer dbx.Close()

	delimStr := "'" + c.splitCharacter + "'"
	if c.splitCharacter == TAB_CHAR_STR {
		delimStr = "E" + delimStr
	}

	var quotes string
	if c.quoteCharacter != "" {
		quotes = fmt.Sprintf("QUOTE '%s'",
			strings.ReplaceAll(c.quoteCharacter, "'", "''"))
	}
	if c.escapeCharacter != "" {
		quotes = fmt.Sprintf("%s ESCAPE '%s'",
			quotes, strings.ReplaceAll(c.escapeCharacter, "'", "''"))
	}

	var copyCmd string
	if c.columns != "" {
		copyCmd = fmt.Sprintf("COPY %s(%s) FROM STDIN WITH DELIMITER %s %s %s", c.getFullTableName(), c.columns, delimStr, quotes, c.copyOptions)
	} else {
		copyCmd = fmt.Sprintf("COPY %s FROM STDIN WITH DELIMITER %s %s %s", c.getFullTableName(), delimStr, quotes, c.copyOptions)
	}
	c.logger.Infof("Copy command: %s", copyCmd)

	for {
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case batch, ok := <-ch:
			if !ok {
				return
			}
			atomic.AddInt64(&c.totalRows, int64(batch.Location.RowCount))

			start := time.Now()
			rows, err := copyFromBatch(ctx, dbx, batch, copyCmd)
			if err != nil {
				handleErr := c.handleCopyError(ctx, dbx, batch, err)
				if handleErr != nil {
					return handleErr
				}
			}
			atomic.AddInt64(&c.insertedRows, rows)

			if err, ok := err.(*ErrBatchAlreadyProcessed); ok {
				if err.State.State == "completed" {
					atomic.AddInt64(&c.skippedRows, int64(batch.Location.RowCount))
				}
			}

			if c.logBatches {
				took := time.Since(start)
				fmt.Printf("[BATCH] starting at row %d, took %v, row count %d, byte len %d, row rate %f/sec\n", batch.Location.StartRow, took, batch.Location.RowCount, batch.Location.ByteLen, float64(batch.Location.RowCount)/float64(took.Seconds()))
			}
		}
	}
}

func (c *Copier) handleCopyError(ctx context.Context, db *sqlx.DB, batch Batch, copyErr error) error {
	errAt := &ErrAtRow{
		Err:           copyErr,
		BatchLocation: batch.Location,
	}

	pgerr := &pgconn.PgError{}
	if errors.As(copyErr, &pgerr) {
		errAt.Row = ExtractRowFrom(pgerr)
	}

	if err, ok := copyErr.(*ErrBatchAlreadyProcessed); ok {
		c.logger.Infof("skip batch %s already processed with state %s", batch.Location, err.State.State)
		return nil
	}

	var failHandlerError *BatchError
	// If failHandler is defined, attempt to handle the error
	if c.failHandler != nil {
		failHandlerError = c.failHandler(batch, errAt)
		if failHandlerError == nil {
			// If fail handler error does not return an error,
			// make it so it recovers the previous error and continues execution
			failHandlerError = NewErrContinue(errAt)
		}
	} else {
		failHandlerError = NewErrStop(errAt)
	}

	c.logger.Infof("handling error %#v", failHandlerError)

	if batch.Location.HasImportID() && !isTemporaryError(failHandlerError) {
		connx, err := db.Connx(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to database")
		}
		defer connx.Close()

		tr := newTransactionAt(batch.Location)
		err = tr.setFailed(ctx, connx, failHandlerError.Error())
		if err != nil {
			if !isDuplicateKeyError(err) {
				return fmt.Errorf("failed to set state to failed, %w", err)
			}
		}
	}

	if !failHandlerError.Continue {
		return failHandlerError
	}

	return nil

}

func isTemporaryError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// Temporary errors: connection failures, resource issues
		if pgErr.Code[:2] == "08" {
			return true
		}
		// Consider other cases as needed for temporary errors
	}
	// Check for Go-specific transient errors
	return errors.Is(err, context.DeadlineExceeded)
}

func isDuplicateKeyError(err error) bool {
	pgerr, ok := err.(*pgconn.PgError)
	if !ok {
		return false
	}
	return pgerr.Code == "23505" // Duplicate key error
}

// report periodically prints the write rate in number of rows per second
func (c *Copier) report(ctx context.Context) {
	start := time.Now()
	ticker := time.NewTicker(c.reportingPeriod)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			c.reportingFunction(Report{
				Timestamp:    now,
				StartedAt:    start,
				InsertedRows: c.GetInsertedRows(),
				SkippedRows:  c.GetSkippedRows(),
				TotalRows:    c.GetTotalRows(),
			})

		case <-ctx.Done():
			// Report one last time
			c.reportingFunction(Report{
				Timestamp:    time.Now(),
				StartedAt:    start,
				InsertedRows: c.GetInsertedRows(),
				SkippedRows:  c.GetSkippedRows(),
				TotalRows:    c.GetTotalRows(),
			})
			return
		}
	}
}

func (c *Copier) getFullTableName() string {
	return fmt.Sprintf(`"%s"."%s"`, c.schemaName, c.tableName)
}

func (c *Copier) GetInsertedRows() int64 {
	return atomic.LoadInt64(&c.insertedRows)
}

func (c *Copier) GetSkippedRows() int64 {
	return atomic.LoadInt64(&c.skippedRows)
}

func (c *Copier) GetTotalRows() int64 {
	return atomic.LoadInt64(&c.totalRows)
}

func (c *Copier) HasImportID() bool {
	return c.importID != ""
}
