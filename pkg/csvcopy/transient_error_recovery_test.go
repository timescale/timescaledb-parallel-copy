package csvcopy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type testLogger struct{
	t *testing.T
	messages bytes.Buffer
}

func (l *testLogger) Infof(msg string, args ...interface{}) {
	l.messages.WriteString(fmt.Sprintf(msg, args...) + "\n")
	l.t.Logf(msg, args...)
}

func (l *testLogger) waitForMessage(message string) {
	for !strings.Contains(l.messages.String(), message) {
		time.Sleep(1 * time.Millisecond)
	}
}

// TestDiskSpaceExhaustionAndRecovery tests writing to a database whose disk fills, and then more space is
// made available.
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

	err = setupSchema(ctx, connStr)
	require.NoError(t, err)

	// Create ballast file to consume space
	exitCode, ballastOutput, err := pgContainer.Exec(ctx, []string{
		"dd", "if=/dev/zero", "of=/var/lib/postgresql/data/ballast.dat", "bs=1M", fmt.Sprintf("count=%d", ballastSizeMB),
	}, exec.Multiplexed())
	require.NoError(t, err)
	ballastMsg, _ := io.ReadAll(ballastOutput)
	require.Equal(t, 0, exitCode, ballastMsg)

	// Generate large dataset
	csvData := generateCsvData(rowCount)

	importID := fmt.Sprintf("test-import-%d", time.Now().Unix())

	logger := testLogger{t: t}

	// Start import
	copier, err := NewCopier(
		connStr,
		"metrics",
		WithImportID(importID),
		WithColumns("device_id,timestamp,value"),
		WithBatchSize(5000),
		WithWorkers(1),
		WithLogger(&logger),
		WithVerbose(true),
		WithRetryOnRecoverableError(true),
	)
	require.NoError(t, err)

	go func() {
		// Wait until we see the copier encounter "out of disk" error
		logger.waitForMessage("SQLSTATE 53100")
		// Delete ballast file to free space (simulates disk resize)
		exitCode, _, err = pgContainer.Exec(ctx, []string{
			"rm", "/var/lib/postgresql/data/ballast.dat",
		})
		require.NoError(t, err)
		require.Equal(t, 0, exitCode, "Failed to delete ballast file")
	}()

	result, err := copier.Copy(ctx, bytes.NewBuffer([]byte(csvData)))
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), result.InsertedRows)
	require.Contains(t, logger.messages.String(), "SQLSTATE 53100")
}

// TestDatabaseShutdownScenarios tests that the copier properly fails when the database
// shuts down using different methods (SIGTERM, SIGINT, SIGKILL)
func TestDatabaseShutdownScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database shutdown scenarios in short mode")
	}

	scenarios := []struct {
		name          string
		shutdownCmd   []string
		expectedCodes []string
		description   string
	}{
		{
			name:          "SIGINT_fast_shutdown",
			shutdownCmd:   []string{"pkill", "-INT", "postgres"},
			expectedCodes: []string{"57014", "57P01", "use of closed network connection"},
			description:   "Fast shutdown produces QUERY_CANCELED (57014) or ADMIN_SHUTDOWN (57P01)",
		},
		{
			name:          "SIGQUIT_immediate_shutdown",
			shutdownCmd:   []string{"pkill", "-QUIT", "postgres"},
			expectedCodes: []string{"unexpected EOF"},
			description:   "Immediate shutdown (SIGQUIT) produces connection error",
		},
		{
			name:          "SIGKILL_immediate_crash",
			shutdownCmd:   []string{"pkill", "-KILL", "postgres"},
			expectedCodes: []string{"unexpected EOF"},
			description:   "Immediate crash (OS-level kill) produces CANNOT_CONNECT_NOW (57P03)",
		},
		{
			name:          "pg_terminate_backend",
			shutdownCmd:   nil, // Special case: uses pg_terminate_backend() instead
			expectedCodes: []string{"57P01", "use of closed network connection"},
			description:   "Backend terminated via pg_terminate_backend() produces ADMIN_SHUTDOWN (57P01)",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()

			pgContainer, err := runContainer(t)
			require.NoError(t, err, "Failed to create container")

			connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
			require.NoError(t, err)

			err = setupSchema(ctx, connStr)
			require.NoError(t, err)

			rowCount := 300_000
			csvData := generateCsvData(rowCount)

			logger := testLogger{t: t}

			go func() {
				logger.waitForMessage("startRow: 10000")

				if scenario.shutdownCmd == nil {
					// Use pg_terminate_backend to kill the copy operation
					terminateBackendDuringCopy(t, ctx, connStr)
				} else {
					// Trigger shutdown in background
					triggerDatabaseShutdown(t, pgContainer, ctx, scenario.shutdownCmd)
					// Wait until csvcopy attempts to connect
					logger.waitForMessage("connection refused")
					err = pgContainer.Start(ctx)
					require.NoError(t, err)
				}
			}()

			// Attempt copy (expect failure due to shutdown)
			// Use smaller batch size and fewer workers to slow down the copy
			copier, err := NewCopier(
				connStr,
				"metrics",
				WithColumns("device_id,timestamp,value"),
				WithBatchSize(10000),
				WithWorkers(1),
				WithLogger(&logger),
				WithVerbose(true),
				WithRetryOnRecoverableError(true),
			)
			require.NoError(t, err)

			result, err := copier.Copy(ctx, bytes.NewBuffer([]byte(csvData)))
			require.NoError(t, err)
			require.Equal(t, int64(rowCount), result.InsertedRows)
			assertContainsAnyString(t, logger.messages.String(), scenario.expectedCodes)
		})
	}
}

// TestCopyStartedDuringGracefulShutdown tests that copy operations fail when started
// during graceful shutdown (SIGTERM). This is different from other shutdown tests which
// trigger shutdown during an active copy operation.
func TestCopyStartedDuringGracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping graceful shutdown test in short mode")
	}

	ctx := context.Background()

	// Setup PostgreSQL container
	pgContainer, err := runContainer(t)
	require.NoError(t, err, "Failed to create container")

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	err = setupSchema(ctx, connStr)
	require.NoError(t, err)

	// Open an idle connection - this prevents PostgreSQL from shutting down immediately
	// Graceful shutdown will wait for this connection to close
	blockingConn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)

	// Trigger graceful shutdown (SIGTERM)
	// PostgreSQL will stop accepting new connections while waiting for existing ones to close
	exitCode, output, err := pgContainer.Exec(ctx,
		[]string{"sh", "-c", "kill -TERM $(head -1 /var/lib/postgresql/data/postmaster.pid)"},
		exec.Multiplexed())
	require.NoError(t, err)
	msg, _ := io.ReadAll(output)
	require.Equal(t, 0, exitCode, "shutdown command failed: %s", msg)

	// Try copy operations at varying delays to catch different shutdown phases
	csvData := generateCsvData(1000)

	logger := testLogger{t: t}

	go func() {
		t.Log("waiting for worker to start")
		logger.waitForMessage("start worker")
		t.Log("closing connection")
		err = blockingConn.Close(ctx)
		if err != nil {
			t.Errorf("failed to close connection: %v", err)
		}
		logger.waitForMessage("connection refused")
		// wait for the connection to close and postgres to stop
		t.Log("starting container")
		err = pgContainer.Start(ctx)
		if err != nil {
			t.Errorf("failed to start container: %v", err)
		}
	}()

	// Attempt to start a copy operation
	copier, err := NewCopier(
		connStr,
		"metrics",
		WithColumns("device_id,timestamp,value"),
		WithBatchSize(100),
		WithWorkers(1),
		WithLogger(&logger),
		WithVerbose(true),
		WithRetryOnRecoverableError(true),
	)
	require.NoError(t, err)

	result, err := copier.Copy(ctx, bytes.NewBuffer([]byte(csvData)))
	require.NoError(t, err)
	require.Equal(t, int64(1000), result.InsertedRows)
}

func setupSchema(ctx context.Context, connStr string) error {
	// Setup database schema
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, `
		CREATE TABLE metrics (
			device_id INT,
			timestamp TIMESTAMPTZ,
			value FLOAT8
		)
	`)
	if err != nil {
		return err
	}
	conn.Close(ctx) //nolint:errcheck
	return nil
}

// assertContainsAnyString verifies that the string contains at least one of the expected error codes
func assertContainsAnyString(t *testing.T, source string, strs []string) {
	t.Helper()
	for _, piece := range strs {
		if strings.Contains(source, piece) {
			return
		}
	}
	t.Fatalf("Expected source to contain one of %v, saw: %v", strs, source)
}

// terminateBackendDuringCopy terminates backend connections using pg_terminate_backend()
func terminateBackendDuringCopy(t *testing.T, ctx context.Context, connStr string) {
	t.Helper()

	adminConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Logf("Warning: could not connect to terminate backends: %v", err)
		return
	}
	defer adminConn.Close(ctx) //nolint:errcheck

	// Terminate all active backends (excluding our admin connection)
	_, err = adminConn.Exec(ctx, `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE state = 'active'
		  AND pid != pg_backend_pid()
		  AND datname = current_database()
	`)
	if err != nil {
		t.Logf("Warning: pg_terminate_backend failed: %v", err)
	}
}

// triggerDatabaseShutdown triggers a database shutdown after a delay using the specified command
func triggerDatabaseShutdown(t *testing.T, pgContainer *postgres.PostgresContainer, ctx context.Context, shutdownCmd []string) {
	t.Helper()

	// Execute shutdown command
	exitCode, output, err := pgContainer.Exec(ctx, shutdownCmd, exec.Multiplexed())

	if err != nil {
		t.Errorf("Failed to execute shutdown command: %v", err)
		return
	}
	msg, _ := io.ReadAll(output)
	if exitCode != 0 {
		t.Errorf("Shutdown command failed with exit code %d: %s", exitCode, msg)
	}
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
		testcontainers.WithConfigModifier(func(config *container.Config) {
			config.Cmd = append(config.Cmd, "-c", "tsdb_admin.protected_roles='foo,bar,baz'")
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err, "Failed to create limited disk container")
	return pgContainer
}

func generateCsvData(rowCount int) string {
	var sb strings.Builder
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < rowCount; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Minute)
		sb.WriteString(fmt.Sprintf("%d,%s,%f\n", i%1000, timestamp.Format(time.RFC3339), rand.Float64()*100))
	}
	return sb.String()
}


func randomPort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close() //nolint:errcheck
	return port, nil
}


func runContainer(t *testing.T, opts ...testcontainers.ContainerCustomizer) (*postgres.PostgresContainer, error) {
	ctx := context.Background()
	t.Helper()

	testId := time.Now().Unix()
	volumeName := fmt.Sprintf("test-volume-%d", testId)

	dockerClient, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatal(err)
	}
	defer dockerClient.Close() //nolint:errcheck

	_, err = dockerClient.VolumeCreate(ctx, volume.CreateOptions{
		Name: volumeName,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		t.Logf("removing container %s", volumeName)
		err = dockerClient.VolumeRemove(context.Background(), volumeName, true)
		if err != nil {
			t.Logf("failed to remove test volume %s", err.Error())
		}
	})

	port, err := randomPort()
	require.NoError(t, err)

	ownOpts := []testcontainers.ContainerCustomizer{
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(30*time.Second)),
		testcontainers.WithConfigModifier(func(config *container.Config) {
			config.Cmd = append(config.Cmd, "-c", "tsdb_admin.protected_roles='foo,bar,baz'")
		}),
		testcontainers.WithHostConfigModifier(func(config *container.HostConfig) {
			config.Mounts = append(config.Mounts, mount.Mount{
				Type:   mount.TypeVolume,
				Source: volumeName,
				Target: "/var/lib/postgresql/data",
			})
			if config.PortBindings == nil {
				config.PortBindings = map[nat.Port][]nat.PortBinding{}
			}
			config.PortBindings["5432/tcp"] = []nat.PortBinding{{HostPort: fmt.Sprintf("%d", port)}}
		}),
	}
	allOpts := slices.Concat(ownOpts, opts)
	pgContainer, err := postgres.Run(ctx,
		"postgres:15.3-alpine",
		allOpts...
	)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		err = pgContainer.Terminate(context.Background())
		if err != nil {
			t.Logf("failed to terminate container %s", err.Error())
		}
	})
	return pgContainer, nil
}
