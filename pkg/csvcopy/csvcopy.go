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

	"github.com/jackc/pgconn"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/timescale/timescaledb-parallel-copy/internal/batch"
	"github.com/timescale/timescaledb-parallel-copy/internal/db"
)

const TAB_CHAR_STR = "\\t"

type Logger interface {
	Infof(msg string, args ...interface{})
}

type noopLogger struct{}

func (l *noopLogger) Infof(msg string, args ...interface{}) {}

type Option func(c *Copier)

func WithLogger(logger Logger) Option {
	return func(c *Copier) {
		c.logger = logger
	}
}

type Result struct {
	RowsRead int64
	Duration time.Duration
	RowRate  float64
}

var HeaderInCopyOptionsError = errors.New("'HEADER' in copyOptions")

type Copier struct {
	dbURL           string
	overrides       []db.Overrideable
	schemaName      string
	tableName       string
	copyOptions     string
	splitCharacter  string
	quoteCharacter  string
	escapeCharacter string
	columns         string
	workers         int
	limit           int64
	batchSize       int
	logBatches      bool
	reportingPeriod time.Duration
	verbose         bool
	skip            int
	logger          Logger
	rowCount        int64
}

func NewCopier(
	dbURL string,
	dbName string,
	schemaName string,
	tableName string,
	copyOptions string,
	splitCharacter string,
	quoteCharacter string,
	escapeCharacter string,
	columns string,
	skipHeader bool,
	headerLinesCnt int,
	workers int,
	limit int64,
	batchSize int,
	logBatches bool,
	reportingPeriod time.Duration,
	verbose bool,
	options ...Option,
) (*Copier, error) {
	var overrides []db.Overrideable
	if dbName != "" {
		overrides = append(overrides, db.OverrideDBName(dbName))
	}

	if strings.Contains(strings.ToUpper(copyOptions), "HEADER") {
		return nil, HeaderInCopyOptionsError
	}

	if len(quoteCharacter) > 1 {
		return nil, errors.New("provided --quote must be a single-byte character")
	}

	if len(escapeCharacter) > 1 {
		return nil, errors.New("provided --escape must be a single-byte character")
	}

	if headerLinesCnt <= 0 {
		return nil, fmt.Errorf(
			"provided --header-line-count (%d) must be greater than 0\n",
			headerLinesCnt,
		)
	}

	skip := 0
	if skipHeader {
		skip = headerLinesCnt
	}

	copier := &Copier{
		dbURL:           dbURL,
		overrides:       overrides,
		schemaName:      schemaName,
		tableName:       tableName,
		copyOptions:     copyOptions,
		splitCharacter:  splitCharacter,
		quoteCharacter:  quoteCharacter,
		escapeCharacter: escapeCharacter,
		columns:         columns,
		workers:         workers,
		limit:           limit,
		batchSize:       batchSize,
		logBatches:      logBatches,
		verbose:         verbose,
		skip:            skip,
		logger:          &noopLogger{},
		rowCount:        0,
	}

	for _, o := range options {
		o(copier)
	}

	if skip > 0 && verbose {
		copier.logger.Infof("Skipping the first %d lines of the input.", headerLinesCnt)
	}

	return copier, nil
}

func (c *Copier) Truncate() (err error) {
	dbx, err := db.Connect(c.dbURL, c.overrides...)
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
	var wg sync.WaitGroup
	batchChan := make(chan batch.Batch, c.workers*2)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, c.workers+1)

	// Generate COPY workers
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.processBatches(ctx, batchChan)
			if err != nil {
				errCh <- err
				cancel()
			}
		}()

	}

	// Reporting thread
	if c.reportingPeriod > (0 * time.Second) {
		go c.report(ctx)
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := batch.Scan(ctx, reader, batchChan, opts); err != nil {
			errCh <- fmt.Errorf("failed reading input: %w", err)
			cancel()
		}
		close(batchChan)
	}()
	wg.Wait()
	close(errCh)
	// We are only interested on the first error message since all other errors
	// must probably are related to the context being canceled.
	err := <-errCh

	end := time.Now()
	took := end.Sub(start)

	rowsRead := atomic.LoadInt64(&c.rowCount)
	rowRate := float64(rowsRead) / float64(took.Seconds())

	return Result{
		RowsRead: rowsRead,
		Duration: took,
		RowRate:  rowRate,
	}, err
}

type ErrAtRow struct {
	Err error
	Row int64
}

func ErrAtRowFromPGError(pgerr *pgconn.PgError, offset int64) *ErrAtRow {
	// Example of Where field
	// "COPY metrics, line 1, column value: \"hello\""
	match := regexp.MustCompile("line (\\d+)").FindStringSubmatch(pgerr.Where)
	if len(match) != 2 {
		return &ErrAtRow{
			Err: pgerr,
			Row: -1,
		}
	}

	line, err := strconv.Atoi(match[1])
	if err != nil {
		return &ErrAtRow{
			Err: pgerr,
			Row: -1,
		}
	}

	return &ErrAtRow{
		Err: pgerr,
		Row: offset + int64(line),
	}
}

func (e *ErrAtRow) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("at row %d, error %s", e.Row, e.Err.Error())
	}
	return fmt.Sprintf("error at row %d", e.Row)
}

func (e *ErrAtRow) Unwrap() error {
	return e.Err
}

// processBatches reads batches from channel c and copies them to the target
// server while tracking stats on the write.
func (c *Copier) processBatches(ctx context.Context, ch chan batch.Batch) (err error) {
	dbx, err := db.Connect(c.dbURL, c.overrides...)
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
				if pgerr, ok := err.(*pgconn.PgError); ok {
					return ErrAtRowFromPGError(pgerr, batch.Location.StartRow)
				}
				return fmt.Errorf("[BATCH] starting at row %d: %w", batch.Location.StartRow, err)
			}
			atomic.AddInt64(&c.rowCount, rows)

			if c.logBatches {
				took := time.Since(start)
				fmt.Printf("[BATCH] starting at row %d, took %v, batch size %d, row rate %f/sec\n", batch.Location.StartRow, took, batch.Location.Length, float64(batch.Location.Length)/float64(took.Seconds()))
			}
		}
	}
}

// report periodically prints the write rate in number of rows per second
func (c *Copier) report(ctx context.Context) {
	start := time.Now()
	prevTime := start
	prevRowCount := int64(0)
	ticker := time.NewTicker(c.reportingPeriod)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			rCount := atomic.LoadInt64(&c.rowCount)

			took := now.Sub(prevTime)
			rowrate := float64(rCount-prevRowCount) / float64(took.Seconds())
			overallRowrate := float64(rCount) / float64(now.Sub(start).Seconds())
			totalTook := now.Sub(start)

			c.logger.Infof(
				"at %v, row rate %0.2f/sec (period), row rate %0.2f/sec (overall), %E total rows",
				totalTook-(totalTook%time.Second),
				rowrate,
				overallRowrate,
				float64(rCount),
			)

			prevRowCount = rCount
			prevTime = now
		case <-ctx.Done():
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
