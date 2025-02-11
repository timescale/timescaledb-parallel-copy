// timescaledb-parallel-copy loads data from CSV format into a TimescaleDB database
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/timescale/timescaledb-parallel-copy/pkg/csvcopy"
)

const (
	binName    = "timescaledb-parallel-copy"
	version    = "v0.10.0"
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

	fromFile            string
	columns             string
	skipHeader          bool
	headerLinesCnt      int
	batchErrorOutputDir string
	skipBatchErrors     bool

	importID        string
	workers         int
	limit           int64
	batchSize       int
	logBatches      bool
	reportingPeriod time.Duration
	verbose         bool
	showVersion     bool

	dbName string
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
	flag.BoolVar(&skipHeader, "skip-header", false, "Skip the first line of the input")
	flag.IntVar(&headerLinesCnt, "header-line-count", 1, "Number of header lines")

	flag.StringVar(&batchErrorOutputDir, "batch-error-output-dir", "", "directory to store batch errors. Settings this will save a .csv file with the contents of the batch that failed and continue with the rest of the data.")
	flag.BoolVar(&skipBatchErrors, "skip-batch-errors", false, "if true, the copy will continue even if a batch fails")

	flag.StringVar(&importID, "import-id", "", "ImportID to guarantee idempotency")
	flag.IntVar(&batchSize, "batch-size", 5000, "Number of rows per insert")
	flag.Int64Var(&limit, "limit", 0, "Number of rows to insert overall; 0 means to insert all")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make")
	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")
	flag.DurationVar(&reportingPeriod, "reporting-period", 0*time.Second, "Period to report insert stats; if 0s, intermediate results will not be reported")
	flag.BoolVar(&verbose, "verbose", false, "Print more information about copying statistics")

	flag.BoolVar(&showVersion, "version", false, "Show the version of this tool")

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
		csvcopy.WithBatchSize(batchSize),
		csvcopy.WithLogBatches(logBatches),
		csvcopy.WithReportingPeriod(reportingPeriod),
		csvcopy.WithVerbose(verbose),
	}

	if importID != "" {
		opts = append(opts, csvcopy.WithImportID(importID))
	}

	batchErrorHandler := csvcopy.BatchHandlerError()
	if skipBatchErrors {
		batchErrorHandler = csvcopy.BatchHandlerNoop()
	}
	if batchErrorOutputDir != "" {
		log.Printf("batch errors will be stored at %s", batchErrorOutputDir)
		batchErrorHandler = csvcopy.BatchHandlerSaveToFile(batchErrorOutputDir, batchErrorHandler)
	}
	if verbose || skipBatchErrors {
		batchErrorHandler = csvcopy.BatchHandlerLog(logger, batchErrorHandler)
	}
	opts = append(opts, csvcopy.WithBatchErrorHandler(batchErrorHandler))

	if skipHeader {
		opts = append(opts,
			csvcopy.WithSkipHeaderCount(headerLinesCnt),
		)
	}

	copier, err := csvcopy.NewCopier(
		postgresConnect,
		tableName,
		opts...,
	)
	if err != nil {
		if errors.Is(err, csvcopy.HeaderInCopyOptionsError) {
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
		defer file.Close()

		reader = file
	} else {
		reader = os.Stdin
	}

	result, err := copier.Copy(context.Background(), reader)
	if err != nil {
		log.Fatal("failed to copy CSV: ", err)
	}

	res := fmt.Sprintf("COPY %d", result.RowsRead)
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
