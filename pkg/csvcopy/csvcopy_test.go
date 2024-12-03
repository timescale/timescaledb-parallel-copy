package csvcopy

import (
	"context"
	"encoding/csv"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

	copier, err := NewCopier(connStr, "test-db", "public", "metrics", "CSV", ",", "", "", "device_id,label,value", false, 1, 1, 0, 5000, true, 0, false)
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

	copier, err := NewCopier(connStr, "test-db", "public", "metrics", "CSV", ",", "", "", "device_id,label,value", false, 1, 1, 0, 2, true, 0, false)
	require.NoError(t, err)
	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	_, err = copier.Copy(context.Background(), reader)
	assert.Error(t, err)
	errAtRow := &ErrAtRow{}
	assert.ErrorAs(t, err, &errAtRow)
	assert.EqualValues(t, 4, errAtRow.Row)
}
