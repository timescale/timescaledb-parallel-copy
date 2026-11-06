# timescaledb-parallel-copy

`timescaledb-parallel-copy` is a command line program for parallelizing
PostgreSQL's built-in `COPY` functionality for bulk inserting data
into [TimescaleDB.](//github.com/timescale/timescaledb/)

## Installation

<details>
<summary>Docker</summary>

```sh
docker pull timescale/timescaledb-parallel-copy
```
</details>

<details>
<summary>Go</summary>

You need the Go runtime (1.13+) installed, then simply `go get` this repo:

```sh
go install github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy@latest
```
</details>

<details>
<summary>Brew</summary>

-   Add the TimescaleDB Homebrew tap.

```sh
brew tap timescale/tap
```

-   Install timescaledb-parallel-copy.

```sh
brew install timescaledb-tools
```
</details>

<details>
<summary>Debian</summary>

-   Install packages needed for the installation.

```sh
sudo apt install gnupg lsb-release wget
```

-   Add the TimescaleDB repository.

```sh
echo "deb https://packagecloud.io/timescale/timescaledb/debian/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
```

-   Install the TimescaleDB GPG key.

```sh
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/timescaledb.gpg
```

-   Install the tools package which contains `timescaledb-parallel-copy`.

```sh
sudo apt install timescaledb-tools
```
</details>

<details>
<summary>Ubuntu</summary>

-   Install packages needed for the installation.

```sh
sudo apt install gnupg lsb-release wget
```

-   Add the TimescaleDB repository.

```sh
echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
```

-   Install the TimescaleDB GPG key.

```sh
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/timescaledb.gpg
```

-   Install the tools package which contains `timescaledb-parallel-copy`.

```sh
sudo apt install timescaledb-tools
```
</details>

<details>
<summary>RedHat</summary>

-   Add the TimescaleDB repository.

```sh
sudo tee /etc/yum.repos.d/timescale_timescaledb.repo <<EOL
[timescale_timescaledb]
name=timescale_timescaledb
baseurl=https://packagecloud.io/timescale/timescaledb/el/$(rpm -E %{rhel})/\$basearch
repo_gpgcheck=1
gpgcheck=0
enabled=1
gpgkey=https://packagecloud.io/timescale/timescaledb/gpgkey
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300
EOL
```

-   Install the tools package which contains `timescaledb-parallel-copy`.

```sh
sudo yum install timescaledb-tools
```
</details>

<details>
<summary>Fedora</summary>

-   Add the TimescaleDB repository.

```sh
sudo tee /etc/yum.repos.d/timescale_timescaledb.repo <<EOL
[timescale_timescaledb]
name=timescale_timescaledb
baseurl=https://packagecloud.io/timescale/timescaledb/el/9/\$basearch
repo_gpgcheck=1
gpgcheck=0
enabled=1
gpgkey=https://packagecloud.io/timescale/timescaledb/gpgkey
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300
EOL
```

-   Install the tools package which contains `timescaledb-parallel-copy`.

```sh
sudo yum install timescaledb-tools
```
</details>

## Usage

Before using this program to bulk insert data, your database should
be installed with the TimescaleDB extension and the target table
should already be made a hypertable.

If you want to bulk insert data from a file named `foo.csv` into a
(hyper)table named `sample` in a database called `test`:

```bash
# single-threaded
$ timescaledb-parallel-copy --connection $DATABASE_URL --table sample --file foo.csv

# 2 workers
$ timescaledb-parallel-copy --connection $DATABASE_URL --table sample --file foo.csv \
    --workers 2

# 2 workers, report progress every 30s
$ timescaledb-parallel-copy --connection $DATABASE_URL --table sample --file foo.csv \
    --workers 2 --reporting-period 30s

# Treat literal string 'NULL' as NULLs:
$ timescaledb-parallel-copy --connection $DATABASE_URL --table sample --file foo.csv \
    --copy-options "NULL 'NULL' CSV"

# Set an import ID to guarantee idempotency
# It can be executed multiple times and the file will be imported only once
$ timescaledb-parallel-copy --connection $DATABASE_URL --table sample --file foo.csv \
    --import-id "import-foo.csv"

```

Other options and flags are also available:

```
$ timescaledb-parallel-copy --help

Usage of timescaledb-parallel-copy:
  -auto-column-mapping
        Automatically map CSV headers to database columns with the same names
  -batch-byte-size int
        Max number of bytes to send in a batch (default 20971520)
  -batch-size int
        Number of rows per insert. It will be limited by batch-byte-size (default 5000)
  -buffer-byte-size int
        Number of bytes to buffer, it has to be big enough to hold a full row (default 2097152)
  -column-mapping string
        Column mapping from CSV to database columns (format: "csv_col1:db_col1,csv_col2:db_col2" or JSON)
  -columns string
        Comma-separated columns present in CSV
  -connection string
        PostgreSQL connection url (default "host=localhost user=postgres sslmode=disable")
  -copy-options string
        Additional options to pass to COPY (e.g., NULL 'NULL') (default "CSV")
  -db-name string
        (deprecated) Database where the destination table exists
  -disable-direct-compress
        Disable using direct compress to write data to TimescaleDB.
  -dangerously-enable-client-side-sorting
        Dangerous: Guarantees that data is pre-sorted, and that every batch is non-overlapping, see TimescaleDB GUC enable_direct_compress_copy_client_sorted
  -escape character
        The ESCAPE character to use during COPY (default '"')
  -file string
        File to read from rather than stdin
  -header-line-count int
        Number of header lines (default 1)
  -import-id string
        ImportID to guarantee idempotency
  -limit int
        Number of rows to insert overall; 0 means to insert all
  -log-batches
        Whether to time individual batches.
  -on-conflict-do-nothing
        Skip duplicate rows on unique constraint violations
  -quote character
        The QUOTE character to use during COPY (default '"')
  -reporting-period duration
        Period to report insert stats; if 0s, intermediate results will not be reported
  -schema string
        Destination table's schema (default "public")
  -skip-batch-errors
        if true, the copy will continue even if a batch fails
  -skip-header
        Skip the first line of the input
  -split string
        Character to split by (default ",")
  -table string
        Destination table for insertions (default "test_table")
  -truncate
        Truncate the destination table before insert
  -verbose
        Print more information about copying statistics
  -version
        Show the version of this tool
  -workers int
        Number of parallel requests to make (default 1)
```


## Streaming

When you omit `--file`, the tool reads from standard input. This lets you
copy data directly from a source into PostgreSQL without intermediate
files or storage.

### Stream from PostgreSQL to PostgreSQL

Use `psql` to run `COPY ... TO STDOUT` on the source database and pipe
the output into `timescaledb-parallel-copy`:

```bash
psql $AWS_SOURCE --csv -c "COPY (SELECT * FROM metrics) TO STDOUT WITH (FORMAT csv, HEADER)" \
  | timescaledb-parallel-copy --connection $TARGET --schema public --table metrics \
      --skip-header --workers 4 --import-id abc
```

### Stream files from Amazon S3

Use the AWS CLI to stream an object to standard output and pipe it into
the tool:

```bash
aws s3 cp s3://my-bucket/metrics.csv - \
  | timescaledb-parallel-copy --connection $TARGET --schema public --table metrics \
      --skip-header --workers 4 --import-id s3-metrics.csv
```

Add `aws s3 cp` to a loop to stream several objects in sequence.

### Stream from MySQL to PostgreSQL

Use the `mysql` client in batch mode to emit tab-separated rows and
pipe them into the tool:

```bash
mysql --batch --skip-column-names -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DB \
  -e "SELECT * FROM metrics" \
  | timescaledb-parallel-copy --connection $TARGET --schema public --table metrics \
      --split $'\t' --copy-options "NULL 'NULL'" --workers 4
```

MySQL batch mode separates columns with tabs and escapes tab, newline,
and backslash characters in values the same way PostgreSQL text format
`COPY` does. The flags above match that format. MySQL prints `NULL`
values as the literal string `NULL`; the `--copy-options` flag tells
PostgreSQL to read that string as `NULL`. One restriction remains: a
text value equal to the literal string `NULL` is imported as `NULL`.

## Purpose

PostgreSQL native `COPY` function is transactional and single-threaded, and may not be suitable for ingesting large
amounts of data. Assuming the file is at least loosely chronologically ordered with respect to the hypertable's time
dimension, this tool should give you great performance gains by parallelizing this operation, allowing users to take
full advantage of their hardware.

This tool also takes care to ingest data in a more efficient manner by roughly preserving the order of the rows. By
taking a "round-robin" approach to sharing inserts between parallel workers, the database has to switch between chunks
less often. This improves memory management and keeps operations on the disk as sequential as possible.

## Contributing

We welcome contributions to this utility, which like TimescaleDB is released under the Apache2 Open Source License. The same [Contributors Agreement](//github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md) applies; please sign the [Contributor License Agreement](https://cla-assistant.io/timescale/timescaledb-parallel-copy) (CLA) if you're a new contributor.

## Running Tests

Some of the tests require a running Postgres database. Set the `TEST_CONNINFO`
environment variable to point at the database you want to run tests against.
(Assume that the tests may be destructive; in particular it is not advisable to
point the tests at any production database.)

For example:

```
$ createdb gotest
$ TEST_CONNINFO='dbname=gotest user=myuser' go test -v ./...
```

## Advanced usage

### Idempotency with `--import-id`

Pass `--import-id` to make an import idempotent. The tool records each
committed batch in the table `timescaledb_parallel_copy.transactions`
under the given ID. If you run the same command again with the same ID,
already committed batches are skipped. If a copy is interrupted, rerun
the same command and it resumes from the last committed batch.

This is useful for immutable sources, such as files in Amazon S3, and
for append-only tables that you copy in ranges. For example, copy rows
with `time < now()` on a schedule with a fixed ID per range. If the
copy fails partway, the next run resumes from the last committed
transaction cursor instead of starting over.

```bash
# Safe to rerun: committed batches are skipped
timescaledb-parallel-copy --connection $DATABASE_URL --table metrics --file metrics.csv \
    --import-id "metrics-2024-01"
```

Transaction records older than 28 days are removed automatically.

### Column Mapping

The tool exposes two flags `--column-mapping` and `--auto-column-mapping` that allow to handle csv headers in a smart way.

`--column-mapping` allows to specify how the columns from your csv map into database columns. It supports two formats:

**Simple format:**
```bash
# Map CSV columns to database columns with different names
$ timescaledb-parallel-copy --connection $DATABASE_URL --table metrics --file data.csv \
    --column-mapping "timestamp:time,temperature:temp_celsius,humidity:humidity_percent"
```

**JSON format:**
```bash
# Same mapping using JSON format
$ timescaledb-parallel-copy --connection $DATABASE_URL --table metrics --file data.csv \
    --column-mapping '{"timestamp":"time","temperature":"temp_celsius","humidity":"humidity_percent"}'
```

Example CSV file with headers:
```csv
timestamp,temperature,humidity
2023-01-01 00:00:00,20.5,65.2
2023-01-01 01:00:00,21.0,64.8
```

This maps the CSV columns to database columns: `timestamp` → `time`, `temperature` → `temp_celsius`, `humidity` → `humidity_percent`.

`--auto-column-mapping` covers the common case when your csv columns have the same name as your database columns.

```bash
# Automatically map CSV headers to database columns with identical names
$ timescaledb-parallel-copy --connection $DATABASE_URL --table sensors --file sensors.csv \
    --auto-column-mapping
```

Example CSV file with headers matching database columns:
```csv
time,device_id,temperature,humidity
2023-01-01 00:00:00,sensor_001,20.5,65.2
2023-01-01 01:00:00,sensor_002,21.0,64.8
```

Both flags automatically skip the header row and cannot be used together with `--skip-header` or `--columns`.

**Flexible Column Mapping:**

Column mappings can include entries for columns that are not present in the input CSV file. This allows you to use the same mapping configuration across multiple input files with different column sets:

```bash
# Define a comprehensive mapping that works with multiple CSV formats
$ timescaledb-parallel-copy --connection $DATABASE_URL --table sensors --file partial_data.csv \
    --column-mapping "timestamp:time,temp:temperature,humidity:humidity_percent,pressure:pressure_hpa,location:device_location"
```

Example CSV file with only some of the mapped columns:
```csv
timestamp,temp,humidity
2023-01-01 00:00:00,20.5,65.2
2023-01-01 01:00:00,21.0,64.8
```

In this case, only the `timestamp`, `temp`, and `humidity` columns from the CSV will be processed and mapped to `time`, `temperature`, and `humidity_percent` respectively. The unused mappings for `pressure` and `location` are simply ignored, allowing the same mapping configuration to work with different input files that may have varying column sets.

You can also map different CSV column names to the same database column, as long as only one of them appears in any given input file:

```bash
# Map both 'temp' and 'temperature' to the same database column
$ timescaledb-parallel-copy --connection $DATABASE_URL --table sensors --file data.csv \
    --column-mapping "timestamp:time,temp:temperature,temperature:temperature,humidity:humidity_percent"
```

This allows importing from different file formats into the same table:

**File A** (uses 'temp'):
```csv
timestamp,temp,humidity
2023-01-01 00:00:00,20.5,65.2
```

**File B** (uses 'temperature'):
```csv
timestamp,temperature,humidity
2023-01-01 02:00:00,22.1,63.5
```

Both files can use the same mapping configuration and import successfully into the same database table, even though they use different column names for the temperature data. The tool only validates for duplicate database columns among the columns actually present in each specific input file.

### Conflict Resolution

Use `--on-conflict-do-nothing` to automatically skip duplicate rows when unique constraint violations occur:

```bash
# Skip duplicate rows and continue importing
$ timescaledb-parallel-copy --connection $DATABASE_URL --table metrics --file data.csv \
    --on-conflict-do-nothing
```

This uses PostgreSQL's `ON CONFLICT DO NOTHING` clause to ignore rows that would violate unique constraints, allowing the import to continue with just the non-duplicate data.

Note that this statement is not allowed within a `COPY FROM`. The tool will fallback to moving your data into a temporary table and running `INSERT INTO ... SELECT * FROM ... ON CONFLICT DO NOTHING`.

This flag is intended to detect real duplicates and not incremental changes to rows. This means it is safe to use this setting is you expect your data to have duplicate rows, but it is not ok to use this as an ingestion pipeline where you expect updates for the same unique constraint.

