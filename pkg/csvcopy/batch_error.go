package csvcopy

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// BatchHandlerLog prints a log line that reports the error in the given batch
func BatchHandlerLog(log Logger, next BatchErrorHandler) BatchErrorHandler {
	return BatchErrorHandler(func(ctx context.Context, c *Copier, db *sqlx.Conn, batch Batch, reason error) *BatchError {
		c.LogWithContext(ctx, "Batch %d, starting at byte %d with len %d, has error: %s", batch.Location.StartRow, batch.Location.ByteOffset, batch.Location.ByteLen, reason.Error())

		if next != nil {
			return next(ctx, c, db, batch, reason)
		}
		return NewErrContinue(reason)
	})
}

// BatchHandlerNoop no operation
func BatchHandlerNoop() BatchErrorHandler {
	return BatchErrorHandler(func(_ context.Context, _ *Copier, _ *sqlx.Conn, _ Batch, reason error) *BatchError {
		return NewErrContinue(reason)
	})
}

// BatchHandlerError fails the process
func BatchHandlerError() BatchErrorHandler {
	return BatchErrorHandler(func(_ context.Context, _ *Copier, _ *sqlx.Conn, _ Batch, reason error) *BatchError {
		return NewErrStop(reason)
	})
}
