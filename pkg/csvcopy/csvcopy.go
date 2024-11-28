package csvcopy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
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

// WithReportingFunction sets the function that will be called at
// reportingPeriod with information about the copy progress
func WithReportingFunction(f ReportFunc) Option {
	return func(c *Copier) {
		c.reportingFunction = f
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
	return func(c *Copier) {
		c.failedBatchDestination = destination
	}
}

type OSWriteFS struct {
	Root string
}

func (fs OSWriteFS) CreateFile(name string) (io.WriteCloser, error) {
	return os.Create(path.Join(fs.Root, name))
}

func WithSkipFailedBatchDir(dir string) Option {
	return func(c *Copier) {
		c.failedBatchDestination = OSWriteFS{Root: dir}
	}
}

type Result struct {
	RowsRead int64
	Duration time.Duration
	RowRate  float64
}

var HeaderInCopyOptionsError = errors.New("'HEADER' in copyOptions")

type Copier struct {
	dbURL             string
	overrides         []db.Overrideable
	schemaName        string
	tableName         string
	copyOptions       string
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
	logger            Logger
	rowCount          int64

	failedBatchDestination WriteFS
	failedBatchErrors      MultiError
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
		reportingPeriod: reportingPeriod,
	}

	for _, o := range options {
		o(copier)
	}

	if skip > 0 && verbose {
		copier.logger.Infof("Skipping the first %d lines of the input.", headerLinesCnt)
	}

	if copier.reportingFunction == nil {
		copier.reportingFunction = DefaultReportFunc(copier.logger)
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

func (c *Copier) Copy(workerCtx context.Context, reader io.Reader) (Result, error) {
	var workerWg sync.WaitGroup
	var supportWg sync.WaitGroup
	batchChan := make(chan batch.Batch, c.workers*2)

	workerCtx, cancelWorkerCtx := context.WithCancel(workerCtx)
	defer cancelWorkerCtx()

	supportCtx, cancelSupportCtx := context.WithCancel(workerCtx)
	defer cancelSupportCtx()

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
	if len(c.failedBatchErrors) > 0 {
		return result, c.failedBatchErrors
	}
	return result, nil
}

type ErrAtRow struct {
	Err error
	Row int64
}

func ErrAtRowFromPGError(pgerr *pgconn.PgError, offset int64) *ErrAtRow {
	// Example of Where field
	// "COPY metrics, line 1, column value: \"hello\""
	match := regexp.MustCompile(`line (\d+)`).FindStringSubmatch(pgerr.Where)
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
			batch.BuildBackup()

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
				fmt.Printf("[BATCH] starting at row %d, took %v, batch size %d, row rate %f/sec\n", batch.Location.StartRow, took, batch.Location.Length, float64(batch.Location.Length)/float64(took.Seconds()))
			}
		}
	}
}
func (c *Copier) handleCopyError(batch batch.Batch, err error) error {
	if pgerr, ok := err.(*pgconn.PgError); ok {
		err = ErrAtRowFromPGError(pgerr, batch.Location.StartRow)
	} else {
		err = fmt.Errorf("[BATCH] starting at row %d: %w", batch.Location.StartRow, err)
	}

	return c.reportError(batch, err)
}

// reportError will attempt to handle and record the error if failedBatchDestination is set
// otherwise, it just returns the same error to fail the execution
func (c *Copier) reportError(batch batch.Batch, err error) error {
	if c.failedBatchDestination == nil {
		return err
	}
	path := fmt.Sprintf("%d.csv", batch.Location.StartRow)
	c.logger.Infof("failed batch file name %s, %s", path, err.Error())

	c.failedBatchErrors = append(c.failedBatchErrors, BatchError{Err: err, Path: path})

	dst, err := c.failedBatchDestination.CreateFile(path)
	if err != nil {
		return fmt.Errorf("failed to create file to store batch error, %w", err)
	}
	defer dst.Close()
	_, err = io.Copy(dst, &batch.Backup)
	if err != nil {
		return fmt.Errorf("failed to write file to store batch error, %w", err)
	}
	return nil
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
