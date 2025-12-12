package csvcopy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestDiskSpaceExhaustionAndRecovery tests the full workflow of handling disk full errors
func TestDiskSpaceExhaustionAndRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping disk space test in short mode")
	}

	ctx := context.Background()

	diskSizeMB := 100
	ballastSizeMB := 50
	rowCount := 200_000

	// Setup container with small disk
	pgContainer := createLimitedDiskContainer(t, ctx, diskSizeMB)
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("Warning: failed to terminate container: %v", err)
		}
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Setup database schema
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Create metrics table - using simple types to avoid CSV quoting issues
	_, err = conn.Exec(ctx, `
		CREATE TABLE metrics (
			device_id INT,
			timestamp TIMESTAMPTZ,
			value FLOAT8
		)
	`)
	require.NoError(t, err)

	// Create ballast file to consume space
	exitCode, ballastOutput, err := pgContainer.Exec(ctx, []string{
		"dd", "if=/dev/zero", "of=/var/lib/postgresql/data/ballast.dat", "bs=1M", fmt.Sprintf("count=%d", ballastSizeMB),
	}, exec.Multiplexed())
	require.NoError(t, err)
	ballastMsg, _ := io.ReadAll(ballastOutput)
	require.Equal(t, 0, exitCode, ballastMsg)

	// Generate large dataset
	var sb strings.Builder
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < rowCount; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Minute)
		sb.WriteString(fmt.Sprintf("%d,%s,%2f\n", i%1000, timestamp.Format(time.RFC3339), rand.Float64()*100))
	}
	csvFile := sb.String()

	// First import attempt (expect disk full)
	importID := fmt.Sprintf("test-import-%d", time.Now().Unix())

	copier, err := NewCopier(
		connStr,
		"metrics",
		WithImportID(importID),
		WithColumns("device_id,timestamp,value"),
		WithBatchSize(5000),
		WithWorkers(2),
	)
	require.NoError(t, err)

	result, err := copier.Copy(ctx, bytes.NewBuffer([]byte(csvFile)))

	// Verify disk full error occurred
	require.ErrorContains(t, err, "53100")

	// Record how many rows were inserted before failure
	rowsBeforeFailure := result.InsertedRows
	require.Greater(t, rowsBeforeFailure, int64(0), "Should have inserted some rows before disk full")

	// Delete ballast file to free space (simulates disk resize)
	exitCode, _, err = pgContainer.Exec(ctx, []string{
		"rm", "/var/lib/postgresql/data/ballast.dat",
	})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode, "Failed to delete ballast file")

	copier, err = NewCopier(
		connStr,
		"metrics",
		WithImportID(importID),
		WithColumns("device_id,timestamp,value"),
		WithBatchSize(5000),
		WithWorkers(2),
	)
	require.NoError(t, err)

	result, err = copier.Copy(ctx, bytes.NewBuffer([]byte(csvFile)))
	require.NoError(t, err)

	// Verify idempotency worked
	assert.Greater(t, result.SkippedRows, int64(0),
		"Should have skipped already-completed batches")

	// Additional verification: total rows should equal expected
	totalInserted := result.InsertedRows + rowsBeforeFailure
	assert.Equal(t, int64(rowCount), totalInserted,
		"Total inserted rows (first attempt + second attempt) should equal 100k")
}


// createLimitedDiskContainer creates a PostgreSQL container with limited disk space using tmpfs
func createLimitedDiskContainer(t *testing.T, ctx context.Context, diskSizeMB int) *postgres.PostgresContainer {
	t.Helper()

	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),

		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.Tmpfs = map[string]string{
				"/var/lib/postgresql/data": fmt.Sprintf("rw,size=%dm,mode=0700", diskSizeMB),
			}
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err, "Failed to create limited disk container")
	return pgContainer
}

