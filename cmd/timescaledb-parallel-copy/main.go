// timescaledb-parallel-copy loads data from CSV format into a TimescaleDB database
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/timescale/timescaledb-parallel-copy/pkg/csvcopy"
	"github.com/timescale/timescaledb-parallel-copy/pkg/errorhandlers"
)

const (
	binName    = "timescaledb-parallel-copy"
	version    = "v0.13.0" // x-release-please-version
	tabCharStr = "\\t"
)

// Flag vars
var (
	postgresConnect string
	schemaName      string
	tableName       string
	truncate        bool

	copyOptions     string
	splitCharacter  string
	quoteCharacter  string
	escapeCharacter string

	fromFile          string
	columns           string
	columnMapping     string
	autoColumnMapping bool
	skipHeader        bool
	headerLinesCnt    int
	skipLines         int
	skipBatchErrors   bool

	importID        string
	workers         int
	limit           int64
	batchSize       int
	bufferSize      int
	batchByteSize   int
	logBatches      bool
	reportingPeriod time.Duration
	verbose         bool
	showVersion     bool

	onConflictDoNothing bool

	dbName string

	disableDirectCompress bool
	clientSideSorting bool

	windows1252HandlingDisabled bool

	noRetry bool
	retryTimeout time.Duration
)

// Parse args
func init() {
	// Documented https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
	flag.StringVar(&postgresConnect, "connection", "host=localhost user=postgres sslmode=disable", "PostgreSQL connection url")
	flag.StringVar(&dbName, "db-name", "", "(deprecated) Database where the destination table exists")
	flag.StringVar(&tableName, "table", "test_table", "Destination table for insertions")
	flag.StringVar(&schemaName, "schema", "public", "Destination table's schema")
	flag.BoolVar(&truncate, "truncate", false, "Truncate the destination table before insert")

	flag.StringVar(&copyOptions, "copy-options", "CSV", "Additional options to pass to COPY (e.g., NULL 'NULL')")
	flag.StringVar(&splitCharacter, "split", ",", "Character to split by")
	flag.StringVar(&quoteCharacter, "quote", "", "The QUOTE `character` to use during COPY (default '\"')")
	flag.StringVar(&escapeCharacter, "escape", "", "The ESCAPE `character` to use during COPY (default '\"')")
	flag.StringVar(&fromFile, "file", "", "File to read from rather than stdin")
	flag.StringVar(&columns, "columns", "", "Comma-separated columns present in CSV")
	flag.StringVar(&columnMapping, "column-mapping", "", "Column mapping from CSV to database columns (format: \"csv_col1:db_col1,csv_col2:db_col2\" or JSON)")
	flag.BoolVar(&autoColumnMapping, "auto-column-mapping", false, "Automatically map CSV headers to database columns with the same names")

	flag.BoolVar(&skipHeader, "skip-header", false, "Skip the first line of the input")
	flag.IntVar(&headerLinesCnt, "header-line-count", 1, "(deprecated) Number of header lines")
	flag.IntVar(&skipLines, "skip-lines", 0, "Skip the first n lines of the input. it is applied before skip-header")

	flag.BoolVar(&skipBatchErrors, "skip-batch-errors", false, "if true, the copy will continue even if a batch fails")

	flag.StringVar(&importID, "import-id", "", "ImportID to guarantee idempotency")
	flag.IntVar(&batchSize, "batch-size", 5000, "Number of rows per insert. It will be limited by batch-byte-size")
	flag.IntVar(&bufferSize, "buffer-byte-size", 2*1024*1024, "Number of bytes to buffer, it has to be big enough to hold a full row")
	flag.IntVar(&batchByteSize, "batch-byte-size", 4*1024*1024, "Max number of bytes to send in a batch")
	flag.Int64Var(&limit, "limit", 0, "Number of rows to insert overall; 0 means to insert all")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make")
	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")
	flag.DurationVar(&reportingPeriod, "reporting-period", 0*time.Second, "Period to report insert stats; if 0s, intermediate results will not be reported")
	flag.BoolVar(&verbose, "verbose", false, "Print more information about copying statistics")

	flag.BoolVar(&onConflictDoNothing, "on-conflict-do-nothing", false, "Skip duplicate rows on unique constraint violations")

	flag.BoolVar(&showVersion, "version", false, "Show the version of this tool")

	flag.BoolVar(&disableDirectCompress, "disable-direct-compress", false, "Disable using direct compress to write data to TimescaleDB")
	flag.BoolVar(&clientSideSorting, "enable-client-side-sorting", false, "Guaranteed data order in place on the client side")

	flag.BoolVar(&windows1252HandlingDisabled, "disable-windows-1252-handling", false, "Disable automatic encoding handling")

	flag.BoolVar(&noRetry, "no-retry", false, "Disable retrying on recoverable errors")
	flag.DurationVar(&retryTimeout, "retry-timeout", 5*time.Minute, "Abort retrying recoverable errors after this duration is elapsed")

	flag.Parse()
}

type csvCopierLogger struct{}

func (l csvCopierLogger) Infof(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

func main() {
	if showVersion {
		log.Printf("%s %s (%s %s)\n", binName, version, runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	if dbName != "" {
		log.Fatalf("Error: Deprecated flag -db-name is being used. Update -connection to connect to the given database")
	}

	if headerLinesCnt != 1 {
		log.Fatalf("Error: -header-line-count is deprecated. Use -skip-lines instead")
	}


	logger := &csvCopierLogger{}

	opts := []csvcopy.Option{
		csvcopy.WithLogger(logger),
		csvcopy.WithSchemaName(schemaName),
		csvcopy.WithCopyOptions(copyOptions),
		csvcopy.WithSplitCharacter(splitCharacter),
		csvcopy.WithQuoteCharacter(quoteCharacter),
		csvcopy.WithEscapeCharacter(escapeCharacter),
		csvcopy.WithColumns(columns),
		csvcopy.WithWorkers(workers),
		csvcopy.WithLimit(limit),
		csvcopy.WithBufferSize(bufferSize),
		csvcopy.WithBatchByteSize(batchByteSize),
		csvcopy.WithBatchSize(batchSize),
		csvcopy.WithLogBatches(logBatches),
		csvcopy.WithReportingPeriod(reportingPeriod),
		csvcopy.WithVerbose(verbose),
	}

	if importID != "" {
		opts = append(opts, csvcopy.WithImportID(importID))
	}

	if columnMapping != "" {
		mapping, err := parseColumnMapping(columnMapping)
		if err != nil {
			log.Fatalf("Error parsing column mapping: %v", err)
		}
		opts = append(opts, csvcopy.WithColumnMapping(mapping))
	}

	if autoColumnMapping {
		opts = append(opts, csvcopy.WithAutoColumnMapping())
	}

	batchErrorHandler := csvcopy.BatchHandlerError()
	if skipBatchErrors {
		batchErrorHandler = csvcopy.BatchHandlerNoop()
	}

	if onConflictDoNothing {
		batchErrorHandler = errorhandlers.BatchConflictHandler(
			errorhandlers.WithConflictHandlerNext(batchErrorHandler),
		)
	}

	if verbose || skipBatchErrors {
		batchErrorHandler = csvcopy.BatchHandlerLog(logger, batchErrorHandler)
	}
	opts = append(opts, csvcopy.WithBatchErrorHandler(batchErrorHandler))

	if skipLines > 0 {
		opts = append(opts, csvcopy.WithSkipHeaderCount(skipLines))
	}

	if skipHeader {
		opts = append(opts, csvcopy.WithSkipHeader(true))
	}

	opts = append(opts, csvcopy.WithDirectCompress(!disableDirectCompress))

	if clientSideSorting {
		opts = append(opts, csvcopy.WithClientSideSorting(true))
	}

	opts = append(opts, csvcopy.WithWindows1252Handling(!windows1252HandlingDisabled))

	opts = append(opts, csvcopy.WithRetryOnRecoverableError(!noRetry))
	opts = append(opts, csvcopy.WithRetryTimeout(retryTimeout))

	copier, err := csvcopy.NewCopier(
		postgresConnect,
		tableName,
		opts...,
	)
	if err != nil {
		if errors.Is(err, csvcopy.ErrHeaderInCopyOptions) {
			log.Fatalf("Error: 'HEADER' detected in -copy-options. If you were using 'HEADER' with PostgreSQL COPY, use: -skip-header")
		}
		log.Fatal(err)
	}

	if truncate { // Remove existing data from the table
		err = copier.Truncate()
		if err != nil {
			log.Printf("failed to truncate table: %s", err)
		}
	}

	var reader io.Reader
	if len(fromFile) > 0 {
		file, err := os.Open(fromFile)
		if err != nil {
			log.Fatalf("failed to open file: %s", err)
		}
		defer file.Close() //nolint:errcheck

		reader = file
	} else {
		reader = os.Stdin
	}

	result, err := copier.Copy(context.Background(), reader)
	if err != nil {
		log.Fatal("failed to copy CSV: ", err)
	}

	res := fmt.Sprintf("COPY %d", result.InsertedRows)
	if verbose {
		res += fmt.Sprintf(
			", took %v with %d worker(s) (mean rate %f/sec)",
			result.Duration,
			workers,
			result.RowRate,
		)
	}
	fmt.Println(res)
}

// parseColumnMapping parses column mapping string into csvcopy.ColumnsMapping
// Supports two formats:
// 1. Simple: "csv_col1:db_col1,csv_col2:db_col2"
// 2. JSON: {"csv_col1":"db_col1","csv_col2":"db_col2"}
func parseColumnMapping(mappingStr string) (csvcopy.ColumnsMapping, error) {
	if mappingStr == "" {
		return nil, nil
	}

	mappingStr = strings.TrimSpace(mappingStr)

	// Check if it's JSON format (starts with '{')
	if strings.HasPrefix(mappingStr, "{") {
		return parseJSONColumnMapping(mappingStr)
	}

	// Parse simple format: "csv_col1:db_col1,csv_col2:db_col2"
	return parseSimpleColumnMapping(mappingStr)
}

// parseJSONColumnMapping parses JSON format column mapping
func parseJSONColumnMapping(jsonStr string) (csvcopy.ColumnsMapping, error) {
	var mappingMap map[string]string
	if err := json.Unmarshal([]byte(jsonStr), &mappingMap); err != nil {
		return nil, fmt.Errorf("invalid JSON format for column mapping: %w", err)
	}

	var mapping csvcopy.ColumnsMapping
	for csvCol, dbCol := range mappingMap {
		mapping = append(mapping, csvcopy.ColumnMapping{
			CSVColumnName:      csvCol,
			DatabaseColumnName: dbCol,
		})
	}

	return mapping, nil
}

// parseSimpleColumnMapping parses simple format: "csv_col1:db_col1,csv_col2:db_col2"
func parseSimpleColumnMapping(simpleStr string) (csvcopy.ColumnsMapping, error) {
	pairs := strings.Split(simpleStr, ",")
	var mapping csvcopy.ColumnsMapping

	for i, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid column mapping format at position %d: '%s', expected 'csv_column:db_column'", i+1, pair)
		}

		csvCol := strings.TrimSpace(parts[0])
		dbCol := strings.TrimSpace(parts[1])

		if csvCol == "" || dbCol == "" {
			return nil, fmt.Errorf("empty column name in mapping at position %d: '%s'", i+1, pair)
		}

		mapping = append(mapping, csvcopy.ColumnMapping{
			CSVColumnName:      csvCol,
			DatabaseColumnName: dbCol,
		})
	}

	return mapping, nil
}
