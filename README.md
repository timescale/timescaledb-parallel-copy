## timescaledb-parallel-copy

`timescaledb-parallel-copy` is a command line program for parallelizing
PostgreSQL's built-in `COPY` functionality for bulk inserting data
into [TimescaleDB.](//github.com/timescale/timescaledb/)

### Getting started
You need the Go runtime (1.6+) installed, then simply `go get` this repo:
```bash
$ go get github.com/timescale/timescaledb-parallel-copy
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
```

Other options and flags are also available, use
 `timescaledb-parallel-copy --help` for more information.
