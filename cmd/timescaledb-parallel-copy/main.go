// timescaledb-parallel-copy loads data from CSV format into a TimescaleDB database
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
	"github.com/timescale/timescaledb-parallel-copy/internal/db"
)

const (
	binName = "timescaledb-parallel-copy"
	version = "0.3.0-dev"
)

// Flag vars
var (
	postgresConnect string
	overrides       []db.Overrideable
	schemaName      string
	tableName       string
	truncate        bool

	copyOptions    string
	splitCharacter string
	fromFile       string
	columns        string
	skipHeader     bool

	workers         int
	limit           int64
	batchSize       int
	logBatches      bool
	reportingPeriod time.Duration
	verbose         bool
	showVersion     bool

	rowCount int64
)

type batch struct {
	rows []string
}

// Parse args
func init() {
	var dbName string
	flag.StringVar(&postgresConnect, "connection", "host=localhost user=postgres sslmode=disable", "PostgreSQL connection url")
	flag.StringVar(&dbName, "db-name", "", "Database where the destination table exists")
	flag.StringVar(&tableName, "table", "test_table", "Destination table for insertions")
	flag.StringVar(&schemaName, "schema", "public", "Desination table's schema")
	flag.BoolVar(&truncate, "truncate", false, "Truncate the destination table before insert")

	flag.StringVar(&copyOptions, "copy-options", "CSV", "Additional options to pass to COPY (e.g., NULL 'NULL')")
	flag.StringVar(&splitCharacter, "split", ",", "Character to split by")
	flag.StringVar(&fromFile, "file", "", "File to read from rather than stdin")
	flag.StringVar(&columns, "columns", "", "Comma-separated columns present in CSV")
	flag.BoolVar(&skipHeader, "skip-header", false, "Skip the first line of the input")

	flag.IntVar(&batchSize, "batch-size", 5000, "Number of rows per insert")
	flag.Int64Var(&limit, "limit", 0, "Number of rows to insert overall; 0 means to insert all")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make")
	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")
	flag.DurationVar(&reportingPeriod, "reporting-period", 0*time.Second, "Period to report insert stats; if 0s, intermediate results will not be reported")
	flag.BoolVar(&verbose, "verbose", false, "Print more information about copying statistics")

	flag.BoolVar(&showVersion, "version", false, "Show the version of this tool")

	flag.Parse()

	if dbName != "" {
		overrides = append(overrides, db.OverrideDBName(dbName))
	}
}

func getFullTableName() string {
	return fmt.Sprintf(`"%s"."%s"`, schemaName, tableName)
}

func main() {
	if showVersion {
		fmt.Printf("%s %s (%s %s)\n", binName, version, runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	if truncate { // Remove existing data from the table
		dbx, err := db.Connect(postgresConnect, overrides...)
		if err != nil {
			panic(err)
		}
		_, err = dbx.Exec(fmt.Sprintf("TRUNCATE %s", getFullTableName()))
		if err != nil {
			panic(err)
		}

		err = dbx.Close()
		if err != nil {
			panic(err)
		}
	}

	var scanner *bufio.Scanner
	if len(fromFile) > 0 {
		file, err := os.Open(fromFile)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner = bufio.NewScanner(file)
	} else {
		scanner = bufio.NewScanner(os.Stdin)
	}

	var wg sync.WaitGroup
	batchChan := make(chan *batch, workers*2)

	// Generate COPY workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go processBatches(&wg, batchChan)
	}

	// Reporting thread
	if reportingPeriod > (0 * time.Second) {
		go report()
	}

	start := time.Now()
	rowsRead := scan(batchSize, scanner, batchChan)
	close(batchChan)
	wg.Wait()
	end := time.Now()
	took := end.Sub(start)
	rowRate := float64(rowsRead) / float64(took.Seconds())

	res := fmt.Sprintf("COPY %d", rowsRead)
	if verbose {
		res += fmt.Sprintf(", took %v with %d worker(s) (mean rate %f/sec)", took, workers, rowRate)
	}
	fmt.Println(res)
}

// report periodically prints the write rate in number of rows per second
func report() {
	start := time.Now()
	prevTime := start
	prevRowCount := int64(0)

	for now := range time.NewTicker(reportingPeriod).C {
		rCount := atomic.LoadInt64(&rowCount)

		took := now.Sub(prevTime)
		rowrate := float64(rCount-prevRowCount) / float64(took.Seconds())
		overallRowrate := float64(rCount) / float64(now.Sub(start).Seconds())
		totalTook := now.Sub(start)

		fmt.Printf("at %v, row rate %0.2f/sec (period), row rate %0.2f/sec (overall), %E total rows\n", totalTook-(totalTook%time.Second), rowrate, overallRowrate, float64(rCount))

		prevRowCount = rCount
		prevTime = now
	}

}

// scan reads lines from a bufio.Scanner, each which should be in CSV format
// with a delimiter specified by --split (comma by default)
func scan(itemsPerBatch int, scanner *bufio.Scanner, batchChan chan *batch) int64 {
	rows := make([]string, 0, itemsPerBatch)
	var linesRead int64

	if skipHeader {
		if verbose {
			fmt.Println("Skipping the first line of the input.")
		}
		scanner.Scan()
	}

	for scanner.Scan() {
		if limit != 0 && linesRead >= limit {
			break
		}

		rows = append(rows, scanner.Text())
		if len(rows) >= itemsPerBatch { // dispatch to COPY worker & reset
			batchChan <- &batch{rows}
			rows = make([]string, 0, itemsPerBatch)
		}
		linesRead++
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if len(rows) > 0 {
		batchChan <- &batch{rows}
	}

	return linesRead
}

// processBatches reads batches from C and writes them to the target server, while tracking stats on the write.
func processBatches(wg *sync.WaitGroup, c chan *batch) {
	dbx, err := db.Connect(postgresConnect, overrides...)
	if err != nil {
		panic(err)
	}
	defer dbx.Close()

	for batch := range c {
		start := time.Now()

		tx := dbx.MustBegin()
		delimStr := fmt.Sprintf("'%s'", splitCharacter)
		if splitCharacter == "\\t" {
			delimStr = "E" + delimStr
		}
		var copyCmd string
		if columns != "" {
			copyCmd = fmt.Sprintf("COPY %s(%s) FROM STDIN WITH DELIMITER %s %s", getFullTableName(), columns, delimStr, copyOptions)
		} else {
			copyCmd = fmt.Sprintf("COPY %s FROM STDIN WITH DELIMITER %s %s", getFullTableName(), delimStr, copyOptions)
		}

		stmt, err := tx.Prepare(copyCmd)
		if err != nil {
			panic(err)
		}

		// Need to cover the string-ified version of the character to actual character for correct split
		sChar := splitCharacter
		if sChar == "\\t" {
			sChar = "\t"
		}
		for _, line := range batch.rows {
			// For some reason this is only needed for tab splitting
			if sChar == "\t" {
				sp := strings.Split(line, sChar)
				args := make([]interface{}, len(sp))
				for i, v := range sp {
					args[i] = v
				}
				_, err = stmt.Exec(args...)
			} else {
				_, err = stmt.Exec(line)
			}

			if err != nil {
				panic(err)
			}
		}

		atomic.AddInt64(&rowCount, int64(len(batch.rows)))

		err = stmt.Close()
		if err != nil {
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}

		if logBatches {
			took := time.Since(start)
			fmt.Printf("[BATCH] took %v, batch size %d, row rate %f/sec\n", took, batchSize, float64(batchSize)/float64(took.Seconds()))
		}

	}
	wg.Done()
}
