package csvcopy

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timescale/timescaledb-parallel-copy/pkg/buffer"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// contextKey is used for context values to avoid collisions
type contextKey string

const workerIDKey contextKey = "workerID"

// WithWorkerID adds a worker ID to the context
func WithWorkerID(ctx context.Context, workerID int) context.Context {
	return context.WithValue(ctx, workerIDKey, workerID)
}

// GetWorkerIDFromContext extracts the worker ID from context, returns -1 if not found
func GetWorkerIDFromContext(ctx context.Context) int {
	if workerID, ok := ctx.Value(workerIDKey).(int); ok {
		return workerID
	}
	return -1
}

const TAB_CHAR_STR = "\\t"

type HeaderHandling int

const (
	HeaderNone HeaderHandling = iota
	HeaderSkip
	HeaderAutoColumnMapping
	HeaderColumnMapping
)

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
	Logger            Logger
	splitCharacter    string
	quoteCharacter    string
	escapeCharacter   string
	columns           string
	workers           int
	queueSize         int
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
	columnMapping     ColumnsMapping
	useFileHeaders    HeaderHandling
	disableDirectCompress bool

	// Rows that are inserted in the database by this copier instance
	insertedRows int64
	// Rows that are skipped because they were already processed
	skippedRows int64
	// Total rows read from the source
	totalRows int64

	failHandler BatchErrorHandler
}

// LogInfo logs a message with worker ID extracted from context if available
func (c *Copier) LogInfo(ctx context.Context, msg string, args ...interface{}) {
	if !c.verbose {
		return
	}
	if workerID := GetWorkerIDFromContext(ctx); workerID >= 0 {
		c.Logger.Infof("[WORKER-%d] "+msg, append([]interface{}{workerID}, args...)...)
	} else {
		c.Logger.Infof(msg, args...)
	}
}

func (c *Copier) LogError(ctx context.Context, msg string, args ...interface{}) {
	if workerID := GetWorkerIDFromContext(ctx); workerID >= 0 {
		c.Logger.Infof("[WORKER-%d] "+msg, append([]interface{}{workerID}, args...)...)
	} else {
		c.Logger.Infof(msg, args...)
	}
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
		schemaName:           "public",
		Logger:               &noopLogger{},
		copyOptions:          "CSV",
		splitCharacter:       ",",
		quoteCharacter:        "",
		escapeCharacter:       "",
		columns:               "",
		workers:               1,
		queueSize:             0,
		limit:                 0,
		bufferSize:            2 * 1024 * 1024,
		batchByteSize:         4 * 1024 * 1024,
		batchSize:             5000,
		logBatches:            false,
		reportingPeriod:       0,
		verbose:               false,
		skip:                  0,
		importID:              "",
		idempotencyWindow:     28 * 24 * time.Hour, // 4 weeks
		disableDirectCompress: false,
	}

	for _, o := range options {
		err := o(copier)
		if err != nil {
			return nil, fmt.Errorf("failed to execute option %T: %w", o, err)
		}
	}

	if copier.skip > 0 && copier.verbose {
		copier.Logger.Infof("Skipping the first %d lines of the input.", copier.skip)
	}

	if copier.reportingFunction == nil {
		copier.reportingFunction = DefaultReportFunc(copier.Logger)
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
	if c.verbose {
		c.LogInfo(context.TODO(), fmt.Sprintf("Truncating table %s", c.GetFullTableName()))
	}
	_, err = dbx.Exec(fmt.Sprintf("TRUNCATE %s", c.GetFullTableName()))
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	return err
}

func (c *Copier) Copy(ctx context.Context, reader io.Reader) (Result, error) {
	var supportWg sync.WaitGroup
	supportCtx, cancelSupportCtx := context.WithCancel(ctx)
	defer cancelSupportCtx()
	// Reporting thread
	if c.reportingPeriod > (0 * time.Second) {
		c.Logger.Infof("There will be reports every %s", c.reportingPeriod.String())
		supportWg.Add(1)
		go func() {
			defer supportWg.Done()
			c.report(supportCtx)
		}()
	}

	if c.HasImportID() {
		if err := ensureTransactionTable(ctx, c.connString); err != nil {
			return Result{}, fmt.Errorf("failed to ensure transaction table, %w", err)
		}
		c.Logger.Infof("Cleaning old transactions older than %s", c.idempotencyWindow)
		if err := cleanOldTransactions(ctx, c.connString, c.idempotencyWindow); err != nil {
			return Result{}, fmt.Errorf("failed to clean old transactions, %w", err)
		}
	}

	// Setup reader with buffering for header skipping
	bufferSize := 2 * 1024 * 1024 // 2 MB buffer
	if c.bufferSize > 0 {
		bufferSize = c.bufferSize
	}

	counter := &CountReader{Reader: reader}
	bufferedReader := bufio.NewReaderSize(counter, bufferSize)

	if c.useFileHeaders == HeaderSkip {
		c.skip++
	}

	if c.skip > 0 {
		if err := skipLines(bufferedReader, c.skip); err != nil {
			return Result{}, fmt.Errorf("failed to skip lines: %w", err)
		}
	}

	if c.useFileHeaders == HeaderAutoColumnMapping || c.useFileHeaders == HeaderColumnMapping {
		// Increment number of skipped lines to account for the header line
		c.skip++
		if err := c.calculateColumnsFromHeaders(bufferedReader); err != nil {
			return Result{}, fmt.Errorf("failed to calculate columns from headers: %w", err)
		}
	}

	queueSize := c.workers * 2
	if c.queueSize > 0 {
		queueSize = c.queueSize
	}

	var workerWg sync.WaitGroup
	batchChan := make(chan Batch, queueSize)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, c.workers+1)

	// Generate COPY workers
	for i := 0; i < c.workers; i++ {
		workerWg.Add(1)
		go func(i int) {
			defer workerWg.Done()
			// Add worker ID to context for all operations in this worker
			workerCtx := WithWorkerID(ctx, i)
			c.LogInfo(workerCtx, "start worker")
			err := c.processBatches(workerCtx, batchChan, i)
			if err != nil {
				c.LogError(workerCtx, "worker error: %v", err)
				errCh <- err
				cancel()
			}
			c.LogInfo(workerCtx, "stop worker")
		}(i)

	}

	opts := scanOptions{
		Size:          c.batchSize,
		Skip:          c.skip,
		Limit:         c.limit,
		BatchByteSize: c.batchByteSize,
		ImportID:      c.importID,
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
		pool := buffer.NewPool(1+queueSize+c.workers, c.batchByteSize)
		if err := scan(ctx, pool, c.LogInfo, counter, bufferedReader, batchChan, opts); err != nil {
			errCh <- fmt.Errorf("failed reading input: %w", err)
			cancel()
		}
		close(batchChan)
		c.LogInfo(ctx, "stop scan")
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

func parseCSVHeaders(bufferedReader *bufio.Reader, quoteCharacter, escapeCharacter, splitCharacter string) ([]string, error) {
	quote := byte('"')
	if quoteCharacter != "" {
		quote = quoteCharacter[0]
	}
	escape := quote
	if escapeCharacter != "" {
		escape = escapeCharacter[0]
	}

	comma := ','
	if splitCharacter != "" {
		comma = rune(splitCharacter[0])
	}

	return parseHeaders(bufferedReader, quote, escape, comma)
}

func (c *Copier) useAutomaticColumnMapping(headers []string) error {
	quotedHeaders := make([]string, len(headers))
	for i, header := range headers {
		quotedHeaders[i] = pgx.Identifier{header}.Sanitize()
	}
	c.columns = strings.Join(quotedHeaders, ",")
	c.LogInfo(context.TODO(), "automatic column mapping: %s", c.columns)
	return nil
}

func validateColumnMapping(columnMapping ColumnsMapping) error {
	seenMappingCSVColumns := make(map[string]bool)
	for _, mapping := range columnMapping {
		if seenMappingCSVColumns[mapping.CSVColumnName] {
			return fmt.Errorf("duplicate source column name: %q", mapping.CSVColumnName)
		}
		seenMappingCSVColumns[mapping.CSVColumnName] = true
	}
	return nil
}

func buildColumnsFromMapping(headers []string, columnMapping ColumnsMapping) ([]string, error) {
	columns := make([]string, 0, len(headers))
	seenColumns := make(map[string]bool)

	for _, header := range headers {
		dbColumn, ok := columnMapping.Get(header)
		if !ok {
			return nil, fmt.Errorf("column mapping not found for header %s", header)
		}

		sanitizedColumn := pgx.Identifier{dbColumn}.Sanitize()
		if seenColumns[sanitizedColumn] {
			return nil, fmt.Errorf("duplicate database column name: %s", sanitizedColumn)
		}

		seenColumns[sanitizedColumn] = true
		columns = append(columns, sanitizedColumn)
	}

	return columns, nil
}

func (c *Copier) calculateColumnsFromHeaders(bufferedReader *bufio.Reader) error {
	headers, err := parseCSVHeaders(bufferedReader, c.quoteCharacter, c.escapeCharacter, c.splitCharacter)
	if err != nil {
		return fmt.Errorf("failed to parse headers: %w", err)
	}

	if len(c.columnMapping) == 0 {
		return c.useAutomaticColumnMapping(headers)
	}

	if err := validateColumnMapping(c.columnMapping); err != nil {
		return err
	}

	columns, err := buildColumnsFromMapping(headers, c.columnMapping)
	if err != nil {
		return err
	}

	c.columns = strings.Join(columns, ",")
	c.LogInfo(context.TODO(), "Using column mapping: %s", c.columns)
	return nil
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

func (c *Copier) CopyCmd() string {
	return c.CopyCmdWithContext(context.Background())
}

func (c *Copier) CopyCmdWithContext(ctx context.Context) string {
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

	var baseCmd string
	if c.columns != "" {
		baseCmd = fmt.Sprintf("COPY %s(%s) FROM STDIN WITH DELIMITER %s %s %s", c.GetFullTableName(), c.columns, delimStr, quotes, c.copyOptions)
	} else {
		baseCmd = fmt.Sprintf("COPY %s FROM STDIN WITH DELIMITER %s %s %s", c.GetFullTableName(), delimStr, quotes, c.copyOptions)
	}

	// Add worker ID comment if available in context
	if workerID := GetWorkerIDFromContext(ctx); workerID >= 0 {
		baseCmd = fmt.Sprintf("/* Worker-%d */ %s", workerID, baseCmd)
	}

	return baseCmd
}

// processBatches reads batches from channel c and copies them to the target
// server while tracking stats on the write.
func (c *Copier) processBatches(ctx context.Context, ch chan Batch, workerID int) (err error) {
	dbx, err := connect(c.connString)
	if err != nil {
		return err
	}
	defer dbx.Close()

	if c.verbose {
		c.LogInfo(ctx, "connected to service")
		c.LogInfo(ctx, fmt.Sprintf("setting direct compress to '%t' for the session", !c.disableDirectCompress))
	}

	// set Direct Compress GUCs for session 
	if _, err := dbx.Exec(fmt.Sprintf("SET timescaledb.enable_direct_compress_copy=%t", !c.disableDirectCompress)); err != nil {
    	return err
	}

	copyCmd := c.CopyCmdWithContext(ctx)
	c.LogInfo(ctx, "Copy command: %s", copyCmd)

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
			err = c.handleBatch(ctx, batch, dbx, copyCmd)
			if err != nil {
				return err
			}
		}
	}
}

func (c *Copier) handleBatch(ctx context.Context, batch Batch, dbx *sqlx.DB, copyCmd string) error {
	defer batch.Close()
	atomic.AddInt64(&c.totalRows, int64(batch.Location.RowCount))

	if c.logBatches {
		c.LogInfo(ctx, "Processing: starting at row %d: rows count %d, byte len %d",
			batch.Location.StartRow, batch.Location.RowCount, batch.Location.ByteLen)
	}

	start := time.Now()
	rows, err := copyFromBatch(ctx, dbx, batch, copyCmd)
	if err != nil {
		handleResult, handleErr := c.handleCopyError(ctx, dbx, batch, err)
		if handleErr != nil {
			c.LogError(ctx, "Error handler failed for batch %d: %v", batch.Location.StartRow, handleErr)
			return handleErr
		}
		atomic.AddInt64(&c.skippedRows, handleResult.SkippedRows)
		rows = handleResult.InsertedRows
	}
	atomic.AddInt64(&c.insertedRows, rows)

	if c.logBatches {
		took := time.Since(start)
		c.LogInfo(ctx, "Processing: starting at row %d, took %v, row count %d, byte len %d, row rate %f/sec", batch.Location.StartRow, took, batch.Location.RowCount, batch.Location.ByteLen, float64(batch.Location.RowCount)/float64(took.Seconds()))
	}
	return nil
}

type HandleCopyErrorResult struct {
	// Rows actually inserted
	InsertedRows int64
	// Rows found but skipped due to a known reason
	SkippedRows int64
}

func (c *Copier) handleCopyError(ctx context.Context, db *sqlx.DB, batch Batch, copyErr error) (HandleCopyErrorResult, error) {
	errAt := &ErrAtRow{
		Err:           copyErr,
		BatchLocation: batch.Location,
	}

	pgerr := &pgconn.PgError{}
	if errors.As(copyErr, &pgerr) {
		errAt.Row = ExtractRowFrom(pgerr)
	}

	if err, ok := copyErr.(*ErrBatchAlreadyProcessed); ok {
		c.LogInfo(ctx, "skip batch %s already processed with state %s", batch.Location, err.State.State)
		if err.State.State == "completed" {
			return HandleCopyErrorResult{
				InsertedRows: 0,
				SkippedRows:  int64(batch.Location.RowCount),
			}, nil
		}
		return HandleCopyErrorResult{
			InsertedRows: 0,
			SkippedRows:  0,
		}, nil

	}

	connx, err := db.Connx(ctx)
	if err != nil {
		return HandleCopyErrorResult{}, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer connx.Close()

	if !batch.Location.HasImportID() {
		if c.failHandler == nil {
			return HandleCopyErrorResult{
				InsertedRows: 0,
				SkippedRows:  0,
			}, errAt
		}

		failHandlerError := c.failHandler(ctx, c, connx, batch, errAt)
		if !failHandlerError.Continue {
			return HandleCopyErrorResult{
				InsertedRows: failHandlerError.InsertedRows,
				SkippedRows:  failHandlerError.SkippedRows,
			}, failHandlerError
		}
		return HandleCopyErrorResult{
			InsertedRows: failHandlerError.InsertedRows,
			SkippedRows:  failHandlerError.SkippedRows,
		}, nil
	}

	// If we have an import ID, we need to start a transaction before the error handling to ensure both can run in the same transaction
	tx, err := connx.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return HandleCopyErrorResult{}, fmt.Errorf("failed to start transaction, %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var failHandlerError HandleBatchErrorResult
	// If failHandler is defined, attempt to handle the error
	if c.failHandler != nil {
		failHandlerError = c.failHandler(ctx, c, connx, batch, errAt)
	} else {
		failHandlerError = NewErrStop(errAt)
	}

	c.LogInfo(ctx, "handling error for batch %s: %#v", batch.Location, failHandlerError)

	tr := newTransactionAt(batch.Location)

	// If the fail handler is marked as handled, the transaction will be marked as completed. Independently if it still contains an error
	if failHandlerError.Handled {
		err = tr.setCompleted(ctx, tx)
		if err != nil {
			return HandleCopyErrorResult{}, fmt.Errorf("failed to set state to completed for batch %s, %w", batch.Location, err)
		}
	} else if !isTemporaryError(failHandlerError.Err) {
		err = tr.setFailed(ctx, tx, failHandlerError.Error())
		if err != nil {
			if !isDuplicateKeyError(err) {
				return HandleCopyErrorResult{}, fmt.Errorf("failed to set state to failed for batch %s, %w", batch.Location, err)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return HandleCopyErrorResult{}, fmt.Errorf("failed to commit transaction for batch %s, %w", batch.Location, err)
	}

	if !failHandlerError.Continue {
		return HandleCopyErrorResult{
			InsertedRows: failHandlerError.InsertedRows,
			SkippedRows:  failHandlerError.SkippedRows,
		}, failHandlerError
	}

	return HandleCopyErrorResult{
		InsertedRows: failHandlerError.InsertedRows,
		SkippedRows:  failHandlerError.SkippedRows,
	}, nil

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

func (c *Copier) GetFullTableName() string {
	return fmt.Sprintf(`"%s"."%s"`, c.schemaName, c.tableName)
}

func (c *Copier) GetTableName() string {
	return c.tableName
}

func (c *Copier) GetSchemaName() string {
	return c.schemaName
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

// ColumnsMapping defines mapping from CSV column name to database column name
type ColumnsMapping []ColumnMapping

func (c ColumnsMapping) Get(header string) (string, bool) {
	for _, mapping := range c {
		if mapping.CSVColumnName == header {
			return mapping.DatabaseColumnName, true
		}
	}
	return "", false
}

// ColumnMapping defines mapping from CSV column name to database column name
type ColumnMapping struct {
	CSVColumnName      string // CSV column name from header
	DatabaseColumnName string // Database column name for COPY statement
}
