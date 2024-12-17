package csvcopy

import (
	"context"
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
	"github.com/timescale/timescaledb-parallel-copy/internal/db"
	"github.com/timescale/timescaledb-parallel-copy/pkg/batch"
)

const TAB_CHAR_STR = "\\t"

type Result struct {
	RowsRead int64
	Duration time.Duration
	RowRate  float64
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
	batchSize         int
	logBatches        bool
	reportingPeriod   time.Duration
	reportingFunction ReportFunc
	verbose           bool
	skip              int
	rowCount          int64

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
		schemaName:      "public",
		logger:          &noopLogger{},
		copyOptions:     "CSV",
		splitCharacter:  ",",
		quoteCharacter:  "",
		escapeCharacter: "",
		columns:         "",
		workers:         1,
		limit:           0,
		batchSize:       5000,
		logBatches:      false,
		reportingPeriod: 0,
		verbose:         false,
		skip:            0,
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
	dbx, err := db.Connect(c.connString)
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
	var workerWg sync.WaitGroup
	batchChan := make(chan batch.Batch, c.workers*2)

	workerCtx, cancelWorkerCtx := context.WithCancel(ctx)
	defer cancelWorkerCtx()

	errCh := make(chan error, c.workers+1)

	// Generate COPY workers
	for i := 0; i < c.workers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			defer c.logger.Infof("worker finished")
			err := c.processBatches(workerCtx, batchChan)
			if err != nil {
				errCh <- err
				cancelWorkerCtx()
			}
		}()

	}

	var supportWg sync.WaitGroup
	supportCtx, cancelSupportCtx := context.WithCancel(ctx)
	defer cancelSupportCtx()
	// Reporting thread
	if c.reportingPeriod > (0 * time.Second) {
		c.logger.Infof("There will be reports every %s", c.reportingPeriod.String())
		supportWg.Add(1)
		go func() {
			defer c.logger.Infof("reporting stopped")
			defer supportWg.Done()
			c.report(supportCtx)
		}()
	}

	opts := batch.Options{
		Size:  c.batchSize,
		Skip:  c.skip,
		Limit: c.limit,
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
		defer c.logger.Infof("scan done")
		if err := batch.Scan(workerCtx, reader, batchChan, opts); err != nil {
			errCh <- fmt.Errorf("failed reading input: %w", err)
			cancelWorkerCtx()
		}
		close(batchChan)
	}()
	c.logger.Infof("waiting for workers to complete")
	workerWg.Wait()

	c.logger.Infof("waiting for support tasks to complete")
	cancelSupportCtx()
	supportWg.Wait()
	close(errCh)
	// We are only interested on the first error message since all other errors
	// must probably are related to the context being canceled.
	err := <-errCh

	end := time.Now()
	took := end.Sub(start)

	rowsRead := atomic.LoadInt64(&c.rowCount)
	rowRate := float64(rowsRead) / float64(took.Seconds())

	result := Result{
		RowsRead: rowsRead,
		Duration: took,
		RowRate:  rowRate,
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
	BatchLocation batch.Location
}

// RowAtLocation returns the row number taking into account the batch location
// so the number matches the original file
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

	return line
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
func (c *Copier) processBatches(ctx context.Context, ch chan batch.Batch) (err error) {
	dbx, err := db.Connect(c.connString)
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

			start := time.Now()
			rows, err := db.CopyFromLines(ctx, dbx, &batch.Data, copyCmd)
			if err != nil {
				err = c.handleCopyError(batch, err)
				if err != nil {
					return err
				}
			}
			atomic.AddInt64(&c.rowCount, rows)

			if c.logBatches {
				took := time.Since(start)
				fmt.Printf("[BATCH] starting at row %d, took %v, batch size %d, row rate %f/sec\n", batch.Location.StartRow, took, batch.Location.RowCount, float64(batch.Location.RowCount)/float64(took.Seconds()))
			}
		}
	}
}
func (c *Copier) handleCopyError(batch batch.Batch, err error) error {
	errAt := &ErrAtRow{
		Err:           err,
		BatchLocation: batch.Location,
	}
	if pgerr, ok := err.(*pgconn.PgError); ok {
		errAt.Row = ExtractRowFrom(pgerr)
	}

	if c.failHandler != nil {
		batch.Rewind()
		return c.failHandler(batch, errAt)
	}
	return errAt

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
				Timestamp: now,
				StartedAt: start,
				RowCount:  c.GetRowCount(),
			})

		case <-ctx.Done():
			// Report one last time
			c.reportingFunction(Report{
				Timestamp: time.Now(),
				StartedAt: start,
				RowCount:  c.GetRowCount(),
			})
			return
		}
	}
}

func (c *Copier) getFullTableName() string {
	return fmt.Sprintf(`"%s"."%s"`, c.schemaName, c.tableName)
}

func (c *Copier) GetRowCount() int64 {
	return atomic.LoadInt64(&c.rowCount)
}
