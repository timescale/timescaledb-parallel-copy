package csvcopy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/timescale/timescaledb-parallel-copy/pkg/buffer"
)

func TestWriteDataToCSV(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	connx, err := db.Connx(ctx)
	require.NoError(t, err)
	defer connx.Close() //nolint:errcheck

	_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"))
	require.NoError(t, err)

	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	require.NoError(t, err)
	require.NotNil(t, r)

	assert.EqualValues(t, 2, int(r.InsertedRows))
	assert.EqualValues(t, 2, int(r.TotalRows))

	var rowCount int64
	err = connx.QueryRowContext(ctx, "select count(*) from public.metrics").Scan(&rowCount)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rowCount)
	assert.Equal(t, int64(2), copier.GetInsertedRows())
	assert.Equal(t, int64(0), copier.GetSkippedRows())

	rows, err := connx.QueryContext(ctx, "select * from public.metrics")
	require.NoError(t, err)

	hasNext := rows.Next()
	require.True(t, hasNext)
	var intValue int
	var strValue string
	var floatValue float64
	err = rows.Scan(&intValue, &strValue, &floatValue)
	require.NoError(t, err)
	assert.Equal(t, 42, intValue)
	assert.Equal(t, "xasev", strValue)
	assert.InDelta(t, 4.2, floatValue, 0, 0o1)

	hasNext = rows.Next()
	require.True(t, hasNext)
	err = rows.Scan(&intValue, &strValue, &floatValue)
	require.NoError(t, err)
	assert.Equal(t, 24, intValue)
	assert.Equal(t, "qased", strValue)
	assert.InDelta(t, 2.4, floatValue, 0, 0o1)

	rows.Close() //nolint:errcheck

	// Check if the table does not exist because the importID was not provided
	var tableExists bool
	err = connx.QueryRowContext(ctx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'timescaledb_parallel_copy.transactions')").Scan(&tableExists)
	require.NoError(t, err)
	require.False(t, tableExists, "Table timescaledb_parallel_copy.transactions exists")
}

func TestWriteDataToCSVWithHeader(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	connx, err := db.Connx(ctx)
	require.NoError(t, err)
	defer connx.Close() //nolint:errcheck

	_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		{"device_id", "label", "value"},
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithSkipHeader(true))
	require.NoError(t, err)

	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	require.NoError(t, err)
	require.NotNil(t, r)

	assert.EqualValues(t, 2, int(r.InsertedRows))
	assert.EqualValues(t, 2, int(r.TotalRows))
	assert.EqualValues(t, 0, int(r.SkippedRows))
	var rowCount int64
	err = connx.QueryRowContext(ctx, "select count(*) from public.metrics").Scan(&rowCount)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rowCount)
	assert.Equal(t, int64(2), copier.GetInsertedRows())

	rows, err := connx.QueryContext(ctx, "select * from public.metrics")
	require.NoError(t, err)

	hasNext := rows.Next()
	require.True(t, hasNext)
	var intValue int
	var strValue string
	var floatValue float64
	err = rows.Scan(&intValue, &strValue, &floatValue)
	require.NoError(t, err)
	assert.Equal(t, 42, intValue)
	assert.Equal(t, "xasev", strValue)
	assert.InDelta(t, 4.2, floatValue, 0, 0o1)

	hasNext = rows.Next()
	require.True(t, hasNext)
	err = rows.Scan(&intValue, &strValue, &floatValue)
	require.NoError(t, err)
	assert.Equal(t, 24, intValue)
	assert.Equal(t, "qased", strValue)
	assert.InDelta(t, 2.4, floatValue, 0, 0o1)

	rows.Close() //nolint:errcheck

	// Check if the table does not exist because the importID was not provided
	var tableExists bool
	err = connx.QueryRowContext(ctx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'timescaledb_parallel_copy.transactions')").Scan(&tableExists)
	require.NoError(t, err)
	require.False(t, tableExists, "Table timescaledb_parallel_copy.transactions exists")
}

func TestErrorAtRow(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithBatchSize(2))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	assert.Error(t, err)

	require.NotNil(t, r)
	assert.EqualValues(t, 2, int(r.InsertedRows))
	assert.EqualValues(t, 4, int(r.TotalRows))
	assert.EqualValues(t, 0, int(r.SkippedRows))

	errAtRow := &ErrAtRow{}
	assert.ErrorAs(t, err, &errAtRow)
	assert.EqualValues(t, 3, errAtRow.RowAtLocation())

	prev := `42,xasev,4.2
24,qased,2.4
`
	assert.EqualValues(t, len(prev), errAtRow.BatchLocation.ByteOffset)
	batch := `24,qased,2.4
24,qased,hello
`
	assert.EqualValues(t, len(batch), errAtRow.BatchLocation.ByteLen)
}

func TestErrorAtRowAndSkipLines(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		{"# This is a comment"},
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithBatchSize(2), WithSkipHeaderCount(1))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	assert.Error(t, err)

	require.NotNil(t, r)
	assert.EqualValues(t, 2, int(r.InsertedRows))
	assert.EqualValues(t, 4, int(r.TotalRows))
	assert.EqualValues(t, 0, int(r.SkippedRows))

	errAtRow := &ErrAtRow{}
	assert.ErrorAs(t, err, &errAtRow)
	assert.EqualValues(t, 4, errAtRow.RowAtLocation())

	prev := `# This is a comment
42,xasev,4.2
24,qased,2.4
`
	assert.EqualValues(t, len(prev), errAtRow.BatchLocation.ByteOffset)
	batch := `24,qased,2.4
24,qased,hello
`
	assert.EqualValues(t, len(batch), errAtRow.BatchLocation.ByteLen)
}

func TestErrorAtRowWithHeader(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		{"number", "text", "float"},
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithSkipHeader(true), WithBatchSize(2))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	assert.Error(t, err)

	require.NotNil(t, r)
	assert.EqualValues(t, 2, int(r.InsertedRows))
	assert.EqualValues(t, 4, int(r.TotalRows))
	assert.EqualValues(t, 0, int(r.SkippedRows))
	errAtRow := &ErrAtRow{}
	assert.ErrorAs(t, err, &errAtRow)
	assert.EqualValues(t, 4, errAtRow.RowAtLocation())

	prev := `number,text,float
42,xasev,4.2
24,qased,2.4
`
	assert.EqualValues(t, len(prev), errAtRow.BatchLocation.ByteOffset)
	batch := `24,qased,2.4
24,qased,hello
`
	assert.EqualValues(t, len(batch), errAtRow.BatchLocation.ByteLen)
}

func TestErrorAtRowAutoColumnMappingAndSkipLines(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		{"# This is a comment"},
		{"# This is another comment"},
		{"# And the following line contain the actual headers"},
		{"device_id", "label", "value"},
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics", WithAutoColumnMapping(), WithSkipHeaderCount(3), WithBatchSize(2))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	assert.Error(t, err)

	require.NotNil(t, r)
	assert.EqualValues(t, 2, int(r.InsertedRows))
	assert.EqualValues(t, 4, int(r.TotalRows))
	assert.EqualValues(t, 0, int(r.SkippedRows))
	errAtRow := &ErrAtRow{}
	assert.ErrorAs(t, err, &errAtRow)
	assert.EqualValues(t, 7, errAtRow.RowAtLocation()) // skipped lines are also counted

	prev := `# This is a comment
# This is another comment
# And the following line contain the actual headers
device_id,label,value
42,xasev,4.2
24,qased,2.4
`
	assert.EqualValues(t, len(prev), errAtRow.BatchLocation.ByteOffset)
	batch := `24,qased,2.4
24,qased,hello
`
	assert.EqualValues(t, len(batch), errAtRow.BatchLocation.ByteLen)
}

func TestErrorAtRowWithColumnMapping(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		{"a", "b", "c"},
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics", WithColumnMapping([]ColumnMapping{
		{CSVColumnName: "a", DatabaseColumnName: "device_id"},
		{CSVColumnName: "b", DatabaseColumnName: "label"},
		{CSVColumnName: "c", DatabaseColumnName: "value"},
	}), WithBatchSize(2))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	assert.Error(t, err)

	require.NotNil(t, r)
	assert.EqualValues(t, 2, int(r.InsertedRows))
	assert.EqualValues(t, 4, int(r.TotalRows))
	assert.EqualValues(t, 0, int(r.SkippedRows))

	errAtRow := &ErrAtRow{}
	assert.ErrorAs(t, err, &errAtRow)
	assert.EqualValues(t, 4, errAtRow.RowAtLocation()) // header line is also counted

	prev := `a,b,c
42,xasev,4.2
24,qased,2.4
`
	assert.EqualValues(t, len(prev), errAtRow.BatchLocation.ByteOffset)
	batch := `24,qased,2.4
24,qased,hello
`
	assert.EqualValues(t, len(batch), errAtRow.BatchLocation.ByteLen)
}

func TestWriteReportProgress(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()
	atLeastOneReport := false
	reportF := func(r Report) {
		atLeastOneReport = true
		require.GreaterOrEqual(t, r.InsertedRows, int64(0))
		require.LessOrEqual(t, r.InsertedRows, int64(2))
	}

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithReportingPeriod(100*time.Millisecond), WithReportingFunction(reportF))
	require.NoError(t, err)

	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	require.NoError(t, err)
	require.NotNil(t, r)

	assert.EqualValues(t, 2, int(r.InsertedRows))
	assert.EqualValues(t, 2, int(r.TotalRows))
	assert.EqualValues(t, 0, int(r.SkippedRows))
	require.True(t, atLeastOneReport)

	var rowCount int64
	err = conn.QueryRow(ctx, "select count(*) from public.metrics").Scan(&rowCount)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rowCount)
	assert.Equal(t, int64(2), copier.GetInsertedRows())

	rows, err := conn.Query(ctx, "select * from public.metrics")
	require.NoError(t, err)

	hasNext := rows.Next()
	require.True(t, hasNext)
	results, err := rows.Values()
	require.NoError(t, err)
	assert.Equal(t, []interface{}{int32(42), "xasev", 4.2}, results)

	hasNext = rows.Next()
	require.True(t, hasNext)
	results, err = rows.Values()
	require.NoError(t, err)
	assert.Equal(t, []interface{}{int32(24), "qased", 2.4}, results)
}

func TestFailedBatchHandlerContinue(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		// Batch 1
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		// Batch 2
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		// Batch 3
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()
	fs := &MockErrorHandler{
		stop: false,
	}

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithBatchSize(2), WithBatchErrorHandler(fs.HandleError))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	result, err := copier.Copy(context.Background(), reader)
	require.NoError(t, err)
	require.EqualValues(t, 4, int(result.InsertedRows))
	require.EqualValues(t, 6, int(result.TotalRows))
	require.EqualValues(t, 0, int(result.SkippedRows))
	require.Contains(t, fs.Errors, 2)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).RowAtLocation(), 3)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).BatchLocation.RowCount, 2)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).BatchLocation.ByteOffset, 26)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).BatchLocation.ByteLen, len("24,qased,2.4\n24,qased,hello\n"))
}

func TestFailedBatchHandlerStop(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		// Batch 1
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		// Batch 2
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		// Batch 3
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()
	fs := &MockErrorHandler{
		stop: true,
	}

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithBatchSize(2), WithBatchErrorHandler(fs.HandleError))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	result, err := copier.Copy(context.Background(), reader)
	require.Error(t, err)
	require.EqualValues(t, 2, int(result.InsertedRows))
	require.EqualValues(t, 4, int(result.TotalRows))
	require.EqualValues(t, 0, int(result.SkippedRows))

	require.Contains(t, fs.Errors, 2)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).RowAtLocation(), 3)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).BatchLocation.RowCount, 2)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).BatchLocation.ByteOffset, 26)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).BatchLocation.ByteLen, len("24,qased,2.4\n24,qased,hello\n"))
}

type MockErrorHandler struct {
	Errors map[int]error
	stop   bool
}

func (fs *MockErrorHandler) HandleError(ctx context.Context, c *Copier, db *sqlx.Conn, batch Batch, reason error) HandleBatchErrorResult {
	if fs.Errors == nil {
		fs.Errors = map[int]error{}
	}
	fs.Errors[int(batch.Location.StartRow)] = reason
	if fs.stop {
		return NewErrStop(reason)
	}
	return NewErrContinue(reason)
}

func TestFailedBatchHandlerFailure(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		// Batch 1
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		// Batch 2
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		// Batch 3
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		err := writer.Write(record)
		require.NoError(t, err, "Error writing record to CSV")
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithBatchSize(2), WithBatchErrorHandler(func(_ context.Context, _ *Copier, _ *sqlx.Conn, _ Batch, err error) HandleBatchErrorResult {
		return NewErrStop(fmt.Errorf("couldn't handle error %w", err))
	}))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	require.Error(t, err)
	require.NotNil(t, r)
	require.EqualValues(t, 2, int(r.InsertedRows))
	require.EqualValues(t, 4, int(r.TotalRows))
	require.EqualValues(t, 0, int(r.SkippedRows))

	require.ErrorContains(t, err, "couldn't handle error")
}

func TestTransactionState(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	connx, err := db.Connx(ctx)
	require.NoError(t, err)
	defer connx.Close() //nolint:errcheck

	_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		// Batch 1
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		// Batch 2
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		// Batch 3
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics",
		WithColumns("device_id,label,value"),
		WithBatchSize(2),
		WithBatchErrorHandler(BatchHandlerNoop()),
		WithImportID("test-file-id"),
	)
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	result, err := copier.Copy(context.Background(), reader)
	require.NoError(t, err)

	assert.EqualValues(t, 6, int(result.TotalRows))
	assert.EqualValues(t, 4, result.InsertedRows)
	assert.EqualValues(t, 0, int(result.SkippedRows))

	batch1, row, err := LoadTransaction(ctx, connx, "test-file-id")
	require.NoError(t, err)
	assert.Equal(t, "test-file-id", row.ImportID)
	assert.Equal(t, int64(0), row.StartRow)
	assert.Equal(t, 2, row.RowCount)
	assert.Equal(t, TransactionRowStateCompleted, row.State)

	batch2, row, err := batch1.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, "test-file-id", row.ImportID)
	assert.Equal(t, int64(2), row.StartRow)
	assert.Equal(t, 2, row.RowCount)
	assert.Equal(t, TransactionRowStateFailed, row.State)
	assert.NotEmpty(t, row.FailureReason)

	batch3, row, err := batch2.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, "test-file-id", row.ImportID)
	assert.Equal(t, int64(4), row.StartRow)
	assert.Equal(t, 2, row.RowCount)
	assert.Equal(t, TransactionRowStateCompleted, row.State)

	batch4, row, err := batch3.Next(ctx, connx)
	require.NoError(t, err)
	require.Nil(t, batch4)
	require.Nil(t, row)
}

func TestTransactionIdempotency(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	connx, err := db.Connx(ctx)
	require.NoError(t, err)
	defer connx.Close() //nolint:errcheck

	_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		// Batch 1
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		// Batch 2
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		// Batch 3
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	copier, err := NewCopier(connStr, "metrics",
		WithColumns("device_id,label,value"),
		WithBatchSize(2),
		WithBatchErrorHandler(BatchHandlerNoop()),
		WithImportID("test-file-id"),
	)
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)

	result, err := copier.Copy(context.Background(), reader)
	require.NoError(t, err)
	// ensure only 4 rows are inserted
	assert.EqualValues(t, 4, result.InsertedRows)
	assert.EqualValues(t, 6, result.TotalRows)
	assert.EqualValues(t, 0, int(result.SkippedRows))

	batch1, row, err := LoadTransaction(ctx, connx, "test-file-id")
	require.NoError(t, err)
	assert.Equal(t, TransactionRowStateCompleted, row.State)

	batch2, row, err := batch1.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, TransactionRowStateFailed, row.State)

	_, row, err = batch2.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, TransactionRowStateCompleted, row.State)

	_, err = tmpfile.Seek(0, 0)
	require.NoError(t, err)

	reader, err = os.Open(tmpfile.Name())
	require.NoError(t, err)

	copier, err = NewCopier(connStr, "metrics",
		WithColumns("device_id,label,value"),
		WithBatchSize(2),
		WithBatchErrorHandler(BatchHandlerNoop()),
		WithImportID("test-file-id"),
	)
	require.NoError(t, err)

	result, err = copier.Copy(context.Background(), reader)
	require.NoError(t, err)
	// ensure no rows are inserted
	assert.EqualValues(t, 0, result.InsertedRows)
	assert.EqualValues(t, 6, result.TotalRows)
	assert.EqualValues(t, 4, int(result.SkippedRows))

	batch1, row, err = LoadTransaction(ctx, connx, "test-file-id")
	require.NoError(t, err)
	assert.Equal(t, TransactionRowStateCompleted, row.State)

	batch2, row, err = batch1.Next(ctx, connx)

	require.NoError(t, err)
	assert.Equal(t, TransactionRowStateFailed, row.State)

	_, row, err = batch2.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, TransactionRowStateCompleted, row.State)

	var total int
	err = connx.QueryRowxContext(ctx, "SELECT COUNT(*) FROM public.metrics").Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, 4, total)

	failedBatchContent := make([]byte, batch2.loc.ByteLen)
	_, err = reader.ReadAt(failedBatchContent, int64(batch2.loc.ByteOffset))
	require.NoError(t, err)
	require.Equal(t, `24,qased,2.4
24,qased,hello
`, string(failedBatchContent))
}

func TestTransactionIdempotencyWindow(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	connx, err := db.Connx(ctx)
	require.NoError(t, err)
	defer connx.Close() //nolint:errcheck

	_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name()) //nolint:errcheck

	// Write data to the CSV file
	writer := csv.NewWriter(tmpfile)

	data := [][]string{
		// Batch 1
		{"42", "xasev", "4.2"},
		{"24", "qased", "2.4"},
		// Batch 2
		{"24", "qased", "2.4"},
		{"24", "qased", "hello"},
		// Batch 3
		{"24", "qased", "2.4"},
		{"24", "qased", "2.4"},
	}

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Error writing record to CSV: %v", err)
		}
	}

	writer.Flush()

	{
		copier, err := NewCopier(connStr, "metrics",
			WithColumns("device_id,label,value"),
			WithBatchSize(2),
			WithBatchErrorHandler(BatchHandlerNoop()),
			WithImportID("test-file-id"),
		)
		require.NoError(t, err)
		reader, err := os.Open(tmpfile.Name())
		require.NoError(t, err)

		result, err := copier.Copy(context.Background(), reader)
		require.NoError(t, err)
		// ensure only 4 rows are inserted
		assert.EqualValues(t, 4, result.InsertedRows)
		assert.EqualValues(t, 6, result.TotalRows)
		assert.EqualValues(t, 0, int(result.SkippedRows))
	}

	// Check idempotency window is working
	{
		_, err = tmpfile.Seek(0, 0)
		require.NoError(t, err)

		reader, err := os.Open(tmpfile.Name())
		require.NoError(t, err)

		copier, err := NewCopier(connStr, "metrics",
			WithColumns("device_id,label,value"),
			WithBatchSize(2),
			WithBatchErrorHandler(BatchHandlerNoop()),
			WithImportID("test-file-id"),
		)
		require.NoError(t, err)

		result, err := copier.Copy(context.Background(), reader)
		require.NoError(t, err)
		// ensure no rows are inserted
		assert.EqualValues(t, 0, result.InsertedRows)
		assert.EqualValues(t, 6, result.TotalRows)
		assert.EqualValues(t, 4, int(result.SkippedRows))
	}

	// Run again with a different idempotency window
	{
		_, err = tmpfile.Seek(0, 0)
		require.NoError(t, err)

		reader, err := os.Open(tmpfile.Name())
		require.NoError(t, err)

		copier, err := NewCopier(connStr, "metrics",
			WithColumns("device_id,label,value"),
			WithBatchSize(2),
			WithBatchErrorHandler(BatchHandlerNoop()),
			WithImportID("test-file-id"),
			WithIdempotencyWindow(1*time.Millisecond), // Ensures cleanup is done
		)
		require.NoError(t, err)

		result, err := copier.Copy(context.Background(), reader)
		require.NoError(t, err)

		// Ensure rows are inserted again
		assert.EqualValues(t, 4, result.InsertedRows)
		assert.EqualValues(t, 6, result.TotalRows)
		assert.EqualValues(t, 0, int(result.SkippedRows))
	}
}

func TestTransactionFailureRetry(t *testing.T) {
	ctx := context.Background()

	t.Run("will succeed", func(t *testing.T) {
		pgContainer, err := postgres.Run(ctx,
			"postgres:15.3-alpine",
			postgres.WithDatabase("test-db"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(5*time.Second)),
		)
		require.NoError(t, err)

		t.Cleanup(func() {
			err := pgContainer.Terminate(ctx)
			require.NoError(t, err)
		})

		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)
		fmt.Println(connStr)

		db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
		require.NoError(t, err)
		defer db.Close() //nolint:errcheck

		connx, err := db.Connx(ctx)
		require.NoError(t, err)
		defer connx.Close() //nolint:errcheck

		_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
		require.NoError(t, err)

		// To force a failure, the best way is to use a bad CSV file. In real life scenario this will probably be caused by
		// temporary database connection errors.
		// The goal is to test that failed transactions are retried.
		// Create a temporary CSV file
		badFile, err := os.CreateTemp("", "example")
		require.NoError(t, err)
		defer os.Remove(badFile.Name()) //nolint:errcheck
		{
			// Write data to the CSV file
			writer := csv.NewWriter(badFile)

			data := [][]string{
				// Batch 1
				{"42", "xasev", "4.2"},
				{"24", "qased", "2.4"},
				// Batch 2
				{"24", "qased", "2.4"},
				{"24", "qased", "forced-failure"},
				// Batch 3
				{"24", "qased", "2.4"},
				{"24", "qased", "2.4"},
			}

			for _, record := range data {
				if err := writer.Write(record); err != nil {
					t.Fatalf("Error writing record to CSV: %v", err)
				}
			}

			writer.Flush()
		}

		// Create a temporary CSV file
		goodFile, err := os.CreateTemp("", "example")
		require.NoError(t, err)
		defer os.Remove(goodFile.Name()) //nolint:errcheck
		{
			// Write data to the CSV file
			writer := csv.NewWriter(goodFile)

			data := [][]string{
				// Batch 1
				{"42", "xasev", "4.2"},
				{"24", "qased", "2.4"},
				// Batch 2
				{"24", "qased", "2.4"},
				{"24", "qased", "2.4"},
				// Batch 3
				{"24", "qased", "2.4"},
				{"24", "qased", "2.4"},
			}

			for _, record := range data {
				if err := writer.Write(record); err != nil {
					t.Fatalf("Error writing record to CSV: %v", err)
				}
			}

			writer.Flush()
		}

		copier, err := NewCopier(connStr, "metrics",
			WithColumns("device_id,label,value"),
			WithBatchSize(2),
			WithBatchErrorHandler(BatchHandlerNoop()),
			WithImportID("test-file-id"),
		)
		require.NoError(t, err)
		reader, err := os.Open(badFile.Name())
		require.NoError(t, err)

		result, err := copier.Copy(context.Background(), reader)
		require.NoError(t, err)
		// ensure only 4 rows are inserted
		assert.EqualValues(t, 4, result.InsertedRows)
		assert.EqualValues(t, 6, result.TotalRows)
		assert.EqualValues(t, 0, int(result.SkippedRows))

		batch1, row, err := LoadTransaction(ctx, connx, "test-file-id")
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateCompleted, row.State)

		batch2, row, err := batch1.Next(ctx, connx)
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateFailed, row.State)

		_, row, err = batch2.Next(ctx, connx)
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateCompleted, row.State)

		reader, err = os.Open(goodFile.Name())
		require.NoError(t, err)

		copier, err = NewCopier(connStr, "metrics",
			WithColumns("device_id,label,value"),
			WithBatchSize(2),
			WithBatchErrorHandler(BatchHandlerNoop()),
			WithImportID("test-file-id"),
		)
		require.NoError(t, err)

		result, err = copier.Copy(context.Background(), reader)
		require.NoError(t, err)
		// ensure no rows are inserted
		assert.EqualValues(t, 2, result.InsertedRows)
		assert.EqualValues(t, 6, result.TotalRows)
		assert.EqualValues(t, 4, int(result.SkippedRows))

		batch1, row, err = LoadTransaction(ctx, connx, "test-file-id")
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateCompleted, row.State)

		batch2, row, err = batch1.Next(ctx, connx)

		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateCompleted, row.State)

		_, row, err = batch2.Next(ctx, connx)
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateCompleted, row.State)

		var total int
		err = connx.QueryRowxContext(ctx, "SELECT COUNT(*) FROM public.metrics").Scan(&total)
		require.NoError(t, err)
		assert.Equal(t, 6, total)
	})

	t.Run("will fail", func(t *testing.T) {
		pgContainer, err := postgres.Run(ctx,
			"postgres:15.3-alpine",
			postgres.WithDatabase("test-db"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(5*time.Second)),
		)
		require.NoError(t, err)

		t.Cleanup(func() {
			err := pgContainer.Terminate(ctx)
			require.NoError(t, err)
		})

		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)
		fmt.Println(connStr)

		db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
		require.NoError(t, err)
		defer db.Close() //nolint:errcheck

		connx, err := db.Connx(ctx)
		require.NoError(t, err)
		defer connx.Close() //nolint:errcheck

		_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
		require.NoError(t, err)

		// Create a temporary CSV file
		badFile, err := os.CreateTemp("", "example")
		require.NoError(t, err)
		defer os.Remove(badFile.Name()) //nolint:errcheck
		{
			// Write data to the CSV file
			writer := csv.NewWriter(badFile)

			data := [][]string{
				// Batch 1
				{"42", "xasev", "4.2"},
				{"24", "qased", "2.4"},
				// Batch 2
				{"24", "qased", "2.4"},
				{"24", "qased", "forced-failure"},
				// Batch 3
				{"24", "qased", "2.4"},
				{"24", "qased", "2.4"},
			}

			for _, record := range data {
				if err := writer.Write(record); err != nil {
					t.Fatalf("Error writing record to CSV: %v", err)
				}
			}

			writer.Flush()
		}

		// Create a temporary CSV file
		retryFile, err := os.CreateTemp("", "example")
		require.NoError(t, err)
		defer os.Remove(retryFile.Name()) //nolint:errcheck
		{
			// Write data to the CSV file
			writer := csv.NewWriter(retryFile)

			data := [][]string{
				// Batch 1
				{"42", "xasev", "4.2"},
				{"24", "qased", "2.4"},
				// Batch 2
				{"24", "qased", "2.4"},
				{"24", "qased", "still fails"},
				// Batch 3
				{"24", "qased", "2.4"},
				{"24", "qased", "2.4"},
			}

			for _, record := range data {
				if err := writer.Write(record); err != nil {
					t.Fatalf("Error writing record to CSV: %v", err)
				}
			}

			writer.Flush()
		}

		copier, err := NewCopier(connStr, "metrics",
			WithColumns("device_id,label,value"),
			WithBatchSize(2),
			WithBatchErrorHandler(BatchHandlerNoop()),
			WithImportID("test-file-id"),
		)
		require.NoError(t, err)
		reader, err := os.Open(badFile.Name())
		require.NoError(t, err)

		result, err := copier.Copy(context.Background(), reader)
		require.NoError(t, err)
		// ensure only 4 rows are inserted
		assert.EqualValues(t, 4, result.InsertedRows)
		assert.EqualValues(t, 6, result.TotalRows)
		assert.EqualValues(t, 0, int(result.SkippedRows))

		batch1, row, err := LoadTransaction(ctx, connx, "test-file-id")
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateCompleted, row.State)

		batch2, row, err := batch1.Next(ctx, connx)
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateFailed, row.State)
		assert.Contains(t, *row.FailureReason, "forced-failure")

		_, row, err = batch2.Next(ctx, connx)
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateCompleted, row.State)

		reader, err = os.Open(retryFile.Name())
		require.NoError(t, err)

		copier, err = NewCopier(connStr, "metrics",
			WithColumns("device_id,label,value"),
			WithBatchSize(2),
			WithBatchErrorHandler(BatchHandlerNoop()),
			WithImportID("test-file-id"),
		)
		require.NoError(t, err)

		result, err = copier.Copy(context.Background(), reader)
		require.NoError(t, err)
		// ensure no rows are inserted
		assert.EqualValues(t, 0, result.InsertedRows)
		assert.EqualValues(t, 6, result.TotalRows)
		assert.EqualValues(t, 4, int(result.SkippedRows))

		batch1, row, err = LoadTransaction(ctx, connx, "test-file-id")
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateCompleted, row.State)

		batch2, row, err = batch1.Next(ctx, connx)

		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateFailed, row.State)
		assert.Contains(t, *row.FailureReason, "still fails")

		_, row, err = batch2.Next(ctx, connx)
		require.NoError(t, err)
		assert.Equal(t, TransactionRowStateCompleted, row.State)

		var total int
		err = connx.QueryRowxContext(ctx, "SELECT COUNT(*) FROM public.metrics").Scan(&total)
		require.NoError(t, err)
		assert.Equal(t, 4, total)
	})
}

func TestCalculateColumnsFromHeaders(t *testing.T) {
	tests := []struct {
		name            string
		csvHeaders      string
		columnMapping   []ColumnMapping
		quoteCharacter  string
		escapeCharacter string
		expectedColumns string
		expectedError   string
	}{
		{
			name:       "simple mapping",
			csvHeaders: "user_id,full_name,email_address",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "user_id", DatabaseColumnName: "id"},
				{CSVColumnName: "full_name", DatabaseColumnName: "name"},
				{CSVColumnName: "email_address", DatabaseColumnName: "email"},
			},
			expectedColumns: "\"id\",\"name\",\"email\"",
		},
		{
			name:       "partial mapping",
			csvHeaders: "id,name,age,email",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "id", DatabaseColumnName: "user_id"},
				{CSVColumnName: "name", DatabaseColumnName: "full_name"},
				{CSVColumnName: "email", DatabaseColumnName: "email_addr"},
			},
			expectedError: "column mapping not found for header age",
		},
		{
			name:       "quoted headers",
			csvHeaders: `"user id","full name","email address"`,
			columnMapping: []ColumnMapping{
				{CSVColumnName: "user id", DatabaseColumnName: "id"},
				{CSVColumnName: "full name", DatabaseColumnName: "name"},
				{CSVColumnName: "email address", DatabaseColumnName: "email"},
			},
			expectedColumns: "\"id\",\"name\",\"email\"",
		},
		{
			name:       "headers with spaces (no quotes)",
			csvHeaders: "user id,full name,email address",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "user id", DatabaseColumnName: "id"},
				{CSVColumnName: "full name", DatabaseColumnName: "name"},
				{CSVColumnName: "email address", DatabaseColumnName: "email"},
			},
			expectedColumns: "\"id\",\"name\",\"email\"",
		},
		{
			name:       "empty header",
			csvHeaders: "id,,email",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "id", DatabaseColumnName: "user_id"},
				{CSVColumnName: "", DatabaseColumnName: "middle_col"},
				{CSVColumnName: "email", DatabaseColumnName: "email_addr"},
			},
			expectedColumns: "\"user_id\",\"middle_col\",\"email_addr\"",
		},
		{
			name:       "single column",
			csvHeaders: "id",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "id", DatabaseColumnName: "user_id"},
			},
			expectedColumns: "\"user_id\"",
		},
		{
			name:       "complex quoted headers with commas",
			csvHeaders: `"user,id","full,name","email,address"`,
			columnMapping: []ColumnMapping{
				{CSVColumnName: "user,id", DatabaseColumnName: "id"},
				{CSVColumnName: "full,name", DatabaseColumnName: "name"},
				{CSVColumnName: "email,address", DatabaseColumnName: "email"},
			},
			expectedColumns: "\"id\",\"name\",\"email\"",
		},
		{
			name:            "custom quote character",
			csvHeaders:      "'user id','full name','email address'",
			quoteCharacter:  "'",
			escapeCharacter: "'",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "user id", DatabaseColumnName: "id"},
				{CSVColumnName: "full name", DatabaseColumnName: "name"},
				{CSVColumnName: "email address", DatabaseColumnName: "email"},
			},
			expectedColumns: "\"id\",\"name\",\"email\"",
		},
		{
			name:       "case sensitive mapping",
			csvHeaders: "ID,Name,Email",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "id", DatabaseColumnName: "user_id"},
				{CSVColumnName: "Name", DatabaseColumnName: "full_name"},
				{CSVColumnName: "Email", DatabaseColumnName: "email_addr"},
			},
			expectedError: "column mapping not found for header ID",
		},
		{
			name:       "order preservation",
			csvHeaders: "email,id,name",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "id", DatabaseColumnName: "user_id"},
				{CSVColumnName: "name", DatabaseColumnName: "full_name"},
				{CSVColumnName: "email", DatabaseColumnName: "email_addr"},
			},
			expectedColumns: "\"email_addr\",\"user_id\",\"full_name\"",
		},
		{
			name:            "no column mapping - use all headers",
			csvHeaders:      `"user id","full name","email address"`,
			columnMapping:   []ColumnMapping{}, // Empty mapping - triggers "No column mapping provided" log
			expectedColumns: "\"user id\",\"full name\",\"email address\"",
		},
		{
			name:       "column mapping with more keys than CSV headers",
			csvHeaders: "id,name",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "id", DatabaseColumnName: "user_id"},
				{CSVColumnName: "name", DatabaseColumnName: "full_name"},
				{CSVColumnName: "email", DatabaseColumnName: "email_addr"}, // Extra mapping key
				{CSVColumnName: "age", DatabaseColumnName: "user_age"},     // Another extra mapping key
			},
			expectedColumns: "\"user_id\",\"full_name\"", // Only mapped columns from CSV headers
		},
		{
			name:       "duplicate database columns in mapping",
			csvHeaders: "first_name,last_name,email",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "first_name", DatabaseColumnName: "name"},
				{CSVColumnName: "last_name", DatabaseColumnName: "name"}, // Same database column
				{CSVColumnName: "email", DatabaseColumnName: "email_addr"},
			},
			expectedError: "duplicate database column name: \"name\". Headers: [first_name last_name email]. Column mapping: [{first_name name} {last_name name} {email email_addr}]",
		},
		{
			name:       "duplicate database columns in mapping but doesn't create a conflict",
			csvHeaders: "first_name,email",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "first_name", DatabaseColumnName: "name"},
				{CSVColumnName: "name", DatabaseColumnName: "name"}, // legacy field mapping exmaple
				{CSVColumnName: "email", DatabaseColumnName: "email_addr"},
			},
			expectedColumns: "\"name\",\"email_addr\"",
		},
		{
			name:       "duplicate csv column name in mapping",
			csvHeaders: "first_name,email",
			columnMapping: []ColumnMapping{
				{CSVColumnName: "first_name", DatabaseColumnName: "name"},
				{CSVColumnName: "first_name", DatabaseColumnName: "first_name"}, // ERROR: it is duplicated
				{CSVColumnName: "email", DatabaseColumnName: "email_addr"},
			},
			expectedError: "duplicate source column name: \"first_name\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copier with the test configuration
			copier := &Copier{
				skip:            1,
				columnMapping:   ColumnsMapping(tt.columnMapping),
				quoteCharacter:  tt.quoteCharacter,
				escapeCharacter: tt.escapeCharacter,
				Logger:          &noopLogger{},
			}

			// Create a buffered reader with the test CSV headers
			csvData := tt.csvHeaders + "\ndata1,data2,data3\n"
			reader := strings.NewReader(csvData)
			counter := &CountReader{Reader: reader}
			bufferedReader := bufio.NewReaderSize(counter, 1024)

			// Call the function under test
			err := copier.calculateColumnsFromHeaders(bufferedReader)

			// Check the results
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedColumns, copier.columns)
			}
		})
	}
}

func TestCalculateColumnsFromHeaders_NoMapping(t *testing.T) {
	// Test the case where no column mapping is provided
	copier := &Copier{
		skip:          1,
		columnMapping: ColumnsMapping{}, // Empty mapping
		Logger:        &noopLogger{},
	}

	csvData := "id,name,email\ndata1,data2,data3\n"
	reader := strings.NewReader(csvData)
	counter := &CountReader{Reader: reader}
	bufferedReader := bufio.NewReaderSize(counter, 1024)

	err := copier.calculateColumnsFromHeaders(bufferedReader)

	require.NoError(t, err)
	assert.Equal(t, "\"id\",\"name\",\"email\"", copier.columns)
}

func TestColumnsMapping_Get(t *testing.T) {
	mapping := ColumnsMapping{
		{CSVColumnName: "user_id", DatabaseColumnName: "id"},
		{CSVColumnName: "full_name", DatabaseColumnName: "name"},
		{CSVColumnName: "email_address", DatabaseColumnName: "email"},
	}

	tests := []struct {
		name           string
		header         string
		expectedColumn string
		expectedFound  bool
	}{
		{
			name:           "existing mapping",
			header:         "user_id",
			expectedColumn: "id",
			expectedFound:  true,
		},
		{
			name:           "another existing mapping",
			header:         "email_address",
			expectedColumn: "email",
			expectedFound:  true,
		},
		{
			name:           "non-existing mapping",
			header:         "age",
			expectedColumn: "",
			expectedFound:  false,
		},
		{
			name:           "empty header",
			header:         "",
			expectedColumn: "",
			expectedFound:  false,
		},
		{
			name:           "case sensitive",
			header:         "USER_ID",
			expectedColumn: "",
			expectedFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			column, found := mapping.Get(tt.header)
			assert.Equal(t, tt.expectedFound, found)
			assert.Equal(t, tt.expectedColumn, column)
		})
	}
}

// This test serves as an example of how the CopyFromBatch implementation is atomic.
func TestAtomicityAssurance(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	// Setup test table
	_, err = db.ExecContext(ctx, "CREATE TABLE public.test_metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Ensure transaction table exists
	err = ensureTransactionTable(ctx, connStr)
	require.NoError(t, err)

	// Create test batch data
	csvLines := bytes.Join([][]byte{
		[]byte("42,test,4.2\n"),
		[]byte("24,data,2.4\n"),
	}, []byte(""))
	seekableData := buffer.NewSeekable(csvLines)

	batch := Batch{
		Data: seekableData,
		Location: Location{
			ImportID:   "test-atomicity-assurance",
			StartRow:   0,
			RowCount:   2,
			ByteOffset: 0,
			ByteLen:    len("42,test,4.2\n24,data,2.4\n"),
		},
	}

	// Test the current implementation (should be atomic)
	testCurrentImplementation := func() error {
		connx, err := db.Connx(ctx)
		if err != nil {
			return fmt.Errorf("acquiring DBx connection for COPY: %w", err)
		}
		defer connx.Close() //nolint:errcheck

		// Use BeginTxx as the current code does
		tx, err := connx.BeginTxx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		defer func() {
			_ = tx.Rollback()
		}()

		// Set transaction control row first
		tr := newTransactionAt(batch.Location)
		err = tr.setCompleted(ctx, tx)
		if err != nil {
			return fmt.Errorf("failed to insert control row, %w", err)
		}

		// Perform COPY operation (this should run within the transaction)
		copyCmd := "COPY public.test_metrics(device_id,label,value) FROM STDIN WITH DELIMITER ',' CSV"
		_, err = CopyFromLines(ctx, connx.Conn, batch.Data, copyCmd)
		if err != nil {
			return fmt.Errorf("failed to copy from lines %w", err)
		}

		// Simulate failure - this should cause rollback of EVERYTHING if atomic
		return fmt.Errorf("simulated failure - should rollback both COPY data and control row")
	}

	// Run the test
	err = testCurrentImplementation()
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated failure")

	// Check what actually happened
	var targetRowCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.test_metrics").Scan(&targetRowCount)
	require.NoError(t, err)

	var controlRowCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM timescaledb_parallel_copy.transactions WHERE import_id = 'test-atomicity-assurance'").Scan(&controlRowCount)
	require.NoError(t, err)

	// This test REQUIRES atomicity - it will FAIL if the implementation is broken
	if targetRowCount != 0 || controlRowCount != 0 {
		t.Errorf("ATOMICITY VIOLATION: Expected both counts to be 0, got targetRows=%d, controlRows=%d",
			targetRowCount, controlRowCount)
		t.Errorf("This means the COPY operation or control row was not properly rolled back")
		t.FailNow()
	}

	// If we get here, atomicity is working correctly
	assert.Equal(t, 0, targetRowCount, "COPY data must be rolled back")
	assert.Equal(t, 0, controlRowCount, "Control row must be rolled back")
	t.Logf("SUCCESS: Current implementation maintains atomicity - both operations rolled back correctly")
}
