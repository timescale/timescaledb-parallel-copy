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
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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

	db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
	require.NoError(t, err)
	defer db.Close()
	connx, err := db.Connx(ctx)
	require.NoError(t, err)
	_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
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

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithImportID("test-file-id"))
	require.NoError(t, err)

	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	r, err := copier.Copy(context.Background(), reader)
	require.NoError(t, err)
	require.NotNil(t, r)

	var rowCount int64
	err = connx.QueryRowContext(ctx, "select count(*) from public.metrics").Scan(&rowCount)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rowCount)
	assert.Equal(t, int64(2), copier.GetRowCount())

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
	assert.InDelta(t, 4.2, floatValue, 0, 01)

	hasNext = rows.Next()
	require.True(t, hasNext)
	err = rows.Scan(&intValue, &strValue, &floatValue)
	require.NoError(t, err)
	assert.Equal(t, 24, intValue)
	assert.Equal(t, "qased", strValue)
	assert.InDelta(t, 2.4, floatValue, 0, 01)

	rows.Close()

	_, controlRow, err := LoadTransaction(ctx, connx, "test-file-id")
	require.NoError(t, err)
	assert.Equal(t, controlRow.State, transactionRowStateCompleted)
	assert.Equal(t, controlRow.FileID, "test-file-id")
	assert.Equal(t, controlRow.StartRow, int64(0))
	assert.Equal(t, controlRow.RowCount, 2)
	assert.Equal(t, controlRow.ByteOffset, 0)
	assert.Equal(t, controlRow.ByteLen, 26)
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
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
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
	require.NoError(t, err)
	require.EqualValues(t, 4, result.RowsRead)

	require.Contains(t, fs.Files, 2)
	require.Equal(t, fs.Files[2].String(), "24,qased,2.4\n24,qased,hello\n")
	require.Contains(t, fs.Errors, 2)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).RowAtLocation(), 3)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).BatchLocation.RowCount, 2)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).BatchLocation.ByteOffset, 26)
	assert.EqualValues(t, fs.Errors[2].(*ErrAtRow).BatchLocation.ByteLen, len("24,qased,2.4\n24,qased,hello\n"))
}

type MockErrorHandler struct {
	Files  map[int]*bytes.Buffer
	Errors map[int]error
}

func (fs *MockErrorHandler) HandleError(batch Batch, reason error) error {
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
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
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

	copier, err := NewCopier(connStr, "metrics", WithColumns("device_id,label,value"), WithBatchSize(2), WithBatchErrorHandler(func(batch Batch, err error) error {
		return fmt.Errorf("couldn't handle error %w", err)
	}))
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	_, err = copier.Copy(context.Background(), reader)
	require.Error(t, err)
	require.ErrorContains(t, err, "couldn't handle error")

}

func TestTransactionState(t *testing.T) {
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
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
	require.NoError(t, err)
	defer db.Close()

	connx, err := db.Connx(ctx)
	require.NoError(t, err)
	defer connx.Close()

	_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
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
	require.EqualValues(t, 4, result.RowsRead)

	batch1, row, err := LoadTransaction(ctx, connx, "test-file-id")
	require.NoError(t, err)
	assert.Equal(t, "test-file-id", row.FileID)
	assert.Equal(t, int64(0), row.StartRow)
	assert.Equal(t, 2, row.RowCount)
	assert.Equal(t, transactionRowStateCompleted, row.State)

	batch2, row, err := batch1.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, "test-file-id", row.FileID)
	assert.Equal(t, int64(2), row.StartRow)
	assert.Equal(t, 2, row.RowCount)
	assert.Equal(t, transactionRowStateFailed, row.State)
	assert.NotEmpty(t, row.FailureReason)

	batch3, row, err := batch2.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, "test-file-id", row.FileID)
	assert.Equal(t, int64(4), row.StartRow)
	assert.Equal(t, 2, row.RowCount)
	assert.Equal(t, transactionRowStateCompleted, row.State)

	batch4, row, err := batch3.Next(ctx, connx)
	require.NoError(t, err)
	require.Nil(t, batch4)
	require.Nil(t, row)

}

func TestTransactionIdempotency(t *testing.T) {
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
	require.NoError(t, err)

	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sqlx.ConnectContext(ctx, "pgx/v5", connStr)
	require.NoError(t, err)
	defer db.Close()

	connx, err := db.Connx(ctx)
	require.NoError(t, err)
	defer connx.Close()

	_, err = connx.ExecContext(ctx, "create table public.metrics (device_id int, label text, value float8)")
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
	assert.EqualValues(t, 4, result.RowsRead)

	batch1, row, err := LoadTransaction(ctx, connx, "test-file-id")
	require.NoError(t, err)
	assert.Equal(t, transactionRowStateCompleted, row.State)

	batch2, row, err := batch1.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, transactionRowStateFailed, row.State)

	_, row, err = batch2.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, transactionRowStateCompleted, row.State)

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
	assert.EqualValues(t, 0, result.RowsRead)

	batch1, row, err = LoadTransaction(ctx, connx, "test-file-id")
	require.NoError(t, err)
	assert.Equal(t, transactionRowStateCompleted, row.State)

	batch2, row, err = batch1.Next(ctx, connx)

	require.NoError(t, err)
	assert.Equal(t, transactionRowStateFailed, row.State)

	_, row, err = batch2.Next(ctx, connx)
	require.NoError(t, err)
	assert.Equal(t, transactionRowStateCompleted, row.State)

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
