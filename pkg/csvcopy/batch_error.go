package csvcopy

import (
	"fmt"
	"io"
	"os"

	"github.com/timescale/timescaledb-parallel-copy/pkg/batch"
)

// BatchHandlerSaveToFile saves the errors to the given directory using the batch start row as file name.
func BatchHandlerSaveToFile(dir string, next BatchErrorHandler) BatchErrorHandler {
	return BatchErrorHandler(func(batch batch.Batch, reason error) error {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to ensure directory exists: %w", err)
		}

		path := fmt.Sprintf("%d.csv", batch.Location.StartRow)

		dst, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("failed to create file to store batch error, %w", err)
		}
		defer dst.Close()

		batch.Rewind()
		_, err = io.Copy(dst, &batch.Data)
		if err != nil {
			return fmt.Errorf("failed to write file to store batch error, %w", err)
		}

		if next != nil {
			return next(batch, reason)
		}
		return nil
	})
}

// BatchHandlerLog prints a log line that reports the error in the given batch
func BatchHandlerLog(log Logger, next BatchErrorHandler) BatchErrorHandler {
	return BatchErrorHandler(func(batch batch.Batch, reason error) error {
		log.Infof("Batch %d has error: %w", batch.Location.StartRow, reason)

		if next != nil {
			return next(batch, reason)
		}
		return nil
	})
}

// BatchHandlerNoop no operation
func BatchHandlerNoop() BatchErrorHandler {
	return BatchErrorHandler(func(_ batch.Batch, _ error) error { return nil })
}
