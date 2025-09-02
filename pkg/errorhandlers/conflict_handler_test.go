package errorhandlers

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
	"github.com/timescale/timescaledb-parallel-copy/pkg/csvcopy"
)

func TestBatchConflictHandler_WithUniqueConstraint(t *testing.T) {
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
	defer conn.Close(ctx)

	// Create table with unique constraint
	_, err = conn.Exec(ctx, `
		CREATE TABLE public.test_metrics (
			device_id int,
			label text,
			value float8,
			UNIQUE(device_id, label)
		)
	`)
	require.NoError(t, err)

	// Create temporary CSV file with duplicate data
	tmpfile, err := os.CreateTemp("", "batch_conflict_test")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	writer := csv.NewWriter(tmpfile)
	data := [][]string{
		// Batch 1 - will succeed
		{"1", "temp", "25.5"},
		{"2", "humidity", "60.0"},
		// Batch 2 - contains conflict
		{"1", "temp", "26.0"}, // Duplicate! Should be skipped
		{"3", "pressure", "1013.25"},
		// Batch 3 - contains another conflict
		{"2", "humidity", "65.0"}, // Another duplicate! Should be skipped
		{"4", "temp", "24.8"},
	}

	for _, record := range data {
		err := writer.Write(record)
		require.NoError(t, err)
	}
	writer.Flush()

	// Test with BatchConflictHandler - should handle conflicts gracefully
	copier, err := csvcopy.NewCopier(connStr, "test_metrics",
		csvcopy.WithColumns("device_id,label,value"),
		csvcopy.WithBatchSize(2),
		csvcopy.WithBatchErrorHandler(BatchConflictHandler(csvcopy.BatchHandlerNoop())),
		csvcopy.WithImportID("test-conflict-handling"),
	)
	require.NoError(t, err)

	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	defer reader.Close()

	result, err := copier.Copy(context.Background(), reader)
	require.NoError(t, err, "Copy should succeed with conflict handler")

	// Verify results
	assert.EqualValues(t, 6, result.TotalRows, "Should process all 6 rows")
	assert.EqualValues(t, 4, result.InsertedRows, "Should insert 4 unique rows")
	assert.EqualValues(t, 2, result.SkippedRows, "No rows should be marked as skipped at the copier level")

	// Verify actual data in database
	var actualCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM public.test_metrics").Scan(&actualCount)
	require.NoError(t, err)
	assert.Equal(t, 4, actualCount, "Should have exactly 4 unique rows in database")

	// Verify specific rows exist (first occurrence of each unique combination)
	rows, err := conn.Query(ctx, "SELECT device_id, label, value FROM public.test_metrics ORDER BY device_id, label")
	require.NoError(t, err)
	defer rows.Close()

	expectedRows := []struct {
		deviceID int
		label    string
		value    float64
	}{
		{1, "temp", 25.5},     // First occurrence
		{2, "humidity", 60.0}, // First occurrence
		{3, "pressure", 1013.25},
		{4, "temp", 24.8},
	}

	i := 0
	for rows.Next() {
		require.Less(t, i, len(expectedRows), "More rows than expected")

		var deviceID int
		var label string
		var value float64

		err = rows.Scan(&deviceID, &label, &value)
		require.NoError(t, err)

		expected := expectedRows[i]
		assert.Equal(t, expected.deviceID, deviceID, "Device ID mismatch at row %d", i)
		assert.Equal(t, expected.label, label, "Label mismatch at row %d", i)
		assert.InDelta(t, expected.value, value, 0.01, "Value mismatch at row %d", i)
		i++
	}
	assert.Equal(t, len(expectedRows), i, "Should have exactly %d rows", len(expectedRows))
}

func TestBatchConflictHandler_WithoutBatchConflictHandler(t *testing.T) {
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
	defer conn.Close(ctx)

	// Create table with unique constraint
	_, err = conn.Exec(ctx, `
		CREATE TABLE public.test_metrics (
			device_id int,
			label text,
			value float8,
			UNIQUE(device_id, label)
		)
	`)
	require.NoError(t, err)

	// Create temporary CSV file with duplicate data (same as previous test)
	tmpfile, err := os.CreateTemp("", "batch_no_batch_conflict_handler_test")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	writer := csv.NewWriter(tmpfile)
	data := [][]string{
		// Batch 1 - will succeed
		{"1", "temp", "25.5"},
		{"2", "humidity", "60.0"},
		// Batch 2 - contains conflict, should cause failure without handler
		{"1", "temp", "26.0"}, // Duplicate! Should cause error
		{"3", "pressure", "1013.25"},
		// Batch 3 - won't be reached due to failure
		{"2", "humidity", "65.0"},
		{"4", "temp", "24.8"},
	}

	for _, record := range data {
		err := writer.Write(record)
		require.NoError(t, err)
	}
	writer.Flush()

	// Test without BatchConflictHandler - should fail on unique constraint violation
	copier, err := csvcopy.NewCopier(connStr, "test_metrics",
		csvcopy.WithColumns("device_id,label,value"),
		csvcopy.WithBatchSize(2),
		csvcopy.WithImportID("test-no-conflict-handling"),
	)
	require.NoError(t, err)

	reader, err := os.Open(tmpfile.Name())
	require.NoError(t, err)
	defer reader.Close()

	result, err := copier.Copy(context.Background(), reader)
	require.Error(t, err, "Copy should fail without conflict handler")

	// Verify error is related to unique constraint violation
	assert.Contains(t, err.Error(), "duplicate key value violates unique constraint",
		"Error should mention unique constraint violation")

	// Verify partial results - first batch should have succeeded
	require.NotNil(t, result)
	assert.EqualValues(t, 2, result.InsertedRows, "Should have inserted first batch (2 rows)")
	assert.EqualValues(t, 4, result.TotalRows, "Should have processed up to the failed batch")

	// Verify actual data in database - only first batch should be there
	var actualCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM public.test_metrics").Scan(&actualCount)
	require.NoError(t, err)
	assert.Equal(t, 2, actualCount, "Should have only 2 rows from first successful batch")

	// Verify the specific rows that were inserted before failure
	rows, err := conn.Query(ctx, "SELECT device_id, label, value FROM public.test_metrics ORDER BY device_id")
	require.NoError(t, err)
	defer rows.Close()

	expectedRows := []struct {
		deviceID int
		label    string
		value    float64
	}{
		{1, "temp", 25.5},
		{2, "humidity", 60.0},
	}

	i := 0
	for rows.Next() {
		require.Less(t, i, len(expectedRows), "More rows than expected")

		var deviceID int
		var label string
		var value float64

		err = rows.Scan(&deviceID, &label, &value)
		require.NoError(t, err)

		expected := expectedRows[i]
		assert.Equal(t, expected.deviceID, deviceID, "Device ID mismatch at row %d", i)
		assert.Equal(t, expected.label, label, "Label mismatch at row %d", i)
		assert.InDelta(t, expected.value, value, 0.01, "Value mismatch at row %d", i)
		i++
	}
	assert.Equal(t, len(expectedRows), i, "Should have exactly %d rows", len(expectedRows))
}
