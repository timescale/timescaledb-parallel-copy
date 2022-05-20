## timescaledb-parallel-copy

`timescaledb-parallel-copy` is a command line program for parallelizing
PostgreSQL's built-in `COPY` functionality for bulk inserting data
into [TimescaleDB.](//github.com/timescale/timescaledb/)

### Getting started
You need the Go runtime (1.6+) installed, then simply `go get` this repo:
```bash
$ go install github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy@latest
```

Before using this program to bulk insert data, your database should
be installed with the TimescaleDB extension and the target table
should already be made a hypertable.

### Using timescaledb-parallel-copy
If you want to bulk insert data from a file named `foo.csv` into a
(hyper)table named `sample` in a database called `test`:

```bash
# single-threaded
$ timescaledb-parallel-copy --db-name test --table sample --file foo.csv

# 2 workers
$ timescaledb-parallel-copy --db-name test --table sample --file foo.csv \
    --workers 2

# 2 workers, report progress every 30s
$ timescaledb-parallel-copy --db-name test --table sample --file foo.csv \
    --workers 2 --reporting-period 30s

# Treat literal string 'NULL' as NULLs:
$ timescaledb-parallel-copy --db-name test --table sample --file foo.csv \
    --copy-options "NULL 'NULL' CSV"
```

Other options and flags are also available:

```
$ timescaledb-parallel-copy --help

Usage of timescaledb-parallel-copy:
  -batch-size int
        Number of rows per insert (default 5000)
  -columns string
        Comma-separated columns present in CSV
  -connection string
        PostgreSQL connection url (default "host=localhost user=postgres sslmode=disable")
  -copy-options string
        Additional options to pass to COPY (e.g., NULL 'NULL') (default "CSV")
  -db-name string
        Database where the destination table exists
  -file string
        File to read from rather than stdin
  -header-line-count int
        Number of header lines (default 1)
  -limit int
        Number of rows to insert overall; 0 means to insert all
  -log-batches
        Whether to time individual batches.
  -reporting-period duration
        Period to report insert stats; if 0s, intermediate results will not be reported
  -schema string
        Destination table's schema (default "public")
  -skip-header
        Skip the first line of the input
  -split string
        Character to split by (default ",")
  -table string
        Destination table for insertions (default "test_table")
  -token-size int
        Maximum size to use for tokens. By default, this is 64KB, so any value less than that will be ignored (default 65536)
  -truncate
        Truncate the destination table before insert
  -verbose
        Print more information about copying statistics
  -version
        Show the version of this tool
  -workers int
        Number of parallel requests to make (default 1)
```

### Contributing
We welcome contributions to this utility, which like TimescaleDB is released under the Apache2 Open Source License.  The same [Contributors Agreement](//github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md) applies; please sign the [Contributor License Agreement](https://cla-assistant.io/timescale/timescaledb-parallel-copy) (CLA) if you're a new contributor.
