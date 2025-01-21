# timescaledb-parallel-copy

`timescaledb-parallel-copy` is a command line program for parallelizing
PostgreSQL's built-in `COPY` functionality for bulk inserting data
into [TimescaleDB.](//github.com/timescale/timescaledb/)

## Installation

### Docker

```sh
docker pull timescale/timescaledb-parallel-copy
```

### Go

You need the Go runtime (1.13+) installed, then simply `go get` this repo:

```sh
go install github.com/timescale/timescaledb-parallel-copy/cmd/timescaledb-parallel-copy@latest
```

### Brew

-   Add the TimescaleDB Homebrew tap.

```sh
brew tap timescale/tap
```

-   Install timescaledb-parallel-copy.

```sh
brew install timescaledb-tools
```

### Debian

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

### Ubuntu

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

### RedHat

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

### Fedora

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
```

Other options and flags are also available:

```
$ timescaledb-parallel-copy --help

Usage of timescaledb-parallel-copy:
  -batch-error-output-dir string
        directory to store batch errors. Settings this will save a .csv file with the contents of the batch that failed and continue with the rest of the data.
  -batch-size int
        Number of rows per insert (default 5000)
  -columns string
        Comma-separated columns present in CSV
  -connection string
        PostgreSQL connection url (default "host=localhost user=postgres sslmode=disable")
  -copy-options string
        Additional options to pass to COPY (e.g., NULL 'NULL') (default "CSV")
  -db-name string
        (deprecated) Database where the destination table exists
  -escape character
        The ESCAPE character to use during COPY (default '"')
  -file string
        File to read from rather than stdin
  -header-line-count int
        Number of header lines (default 1)
  -limit int
        Number of rows to insert overall; 0 means to insert all
  -log-batches
        Whether to time individual batches.
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

### Running Tests

Some of the tests require a running Postgres database. Set the `TEST_CONNINFO`
environment variable to point at the database you want to run tests against.
(Assume that the tests may be destructive; in particular it is not advisable to
point the tests at any production database.)

For example:

```
$ createdb gotest
$ TEST_CONNINFO='dbname=gotest user=myuser' go test -v ./...
```
