package csvcopy

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/timescale/timescaledb-parallel-copy/pkg/batch"
)

func TestWriteDataToCSV(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15.3-alpine"),
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
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

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

	var rowCount int64
	err = conn.QueryRow(ctx, "select count(*) from public.metrics").Scan(&rowCount)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rowCount)
	assert.Equal(t, int64(2), copier.GetRowCount())

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

func TestErrorAtRow(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15.3-alpine"),
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
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

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
	_, err = copier.Copy(context.Background(), reader)
	assert.Error(t, err)
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

func TestErrorAtRowWithHeader(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15.3-alpine"),
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
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

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
	_, err = copier.Copy(context.Background(), reader)
	assert.Error(t, err)
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

func TestWriteReportProgress(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15.3-alpine"),
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
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

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
		require.GreaterOrEqual(t, r.RowCount, int64(0))
		require.LessOrEqual(t, r.RowCount, int64(2))
	}

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithReportingPeriod(100*time.Millisecond), WithReportingFunction(reportF))
	require.NoError(t, err)

	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	require.NoError(t, err)
	require.NotNil(t, r)

	require.True(t, atLeastOneReport)

	var rowCount int64
	err = conn.QueryRow(ctx, "select count(*) from public.metrics").Scan(&rowCount)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rowCount)
	assert.Equal(t, int64(2), copier.GetRowCount())

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

func TestFailedBatchHandler(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15.3-alpine"),
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
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

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
	fs := &MockErrorHandler{}

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithBatchSize(2), WithBatchErrorHandler(fs.HandleError))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	result, err := copier.Copy(context.Background(), reader)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, result.RowsRead)

	require.Contains(t, fs.Files, 1)
	assert.Equal(t, fs.Files[1].String(), "24,qased,2.4\n24,qased,hello\n")
	require.Contains(t, fs.Errors, 1)
	assert.EqualValues(t, fs.Errors[1].(*ErrAtRow).RowAtLocation(), 3)
	assert.EqualValues(t, fs.Errors[1].(*ErrAtRow).BatchLocation.ByteOffset, 26)
}

type MockErrorHandler struct {
	Files  map[int]*bytes.Buffer
	Errors map[int]error
}

func (fs *MockErrorHandler) HandleError(batch batch.Batch, reason error) error {
	if fs.Files == nil {
		fs.Files = map[int]*bytes.Buffer{}
	}
	if fs.Errors == nil {
		fs.Errors = map[int]error{}
	}
	buf := &bytes.Buffer{}
	_, err := buf.ReadFrom(&batch.Data)
	if err != nil {
		return err
	}
	fs.Files[int(batch.Location.StartRow)] = buf
	fs.Errors[int(batch.Location.StartRow)] = reason
	return nil
}

func TestFailedBatchHandlerFailure(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15.3-alpine"),
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
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "create table public.metrics (device_id int, label text, value float8)")
	require.NoError(t, err)

	// Create a temporary CSV file
	tmpfile, err := os.CreateTemp("", "example")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

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

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithBatchSize(2), WithBatchErrorHandler(func(batch batch.Batch, err error) error {
		return fmt.Errorf("couldn't handle error %w", err)
	}))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	_, err = copier.Copy(context.Background(), reader)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "couldn't handle error")

}
