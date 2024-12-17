package csvcopy

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/timescale/timescaledb-parallel-copy/pkg/batch"
)

// BatchHandlerSaveToFile saves the errors to the given directory using the batch start row as file name.
func BatchHandlerSaveToFile(dir string, next BatchErrorHandler) BatchErrorHandler {
	return BatchErrorHandler(func(batch batch.Batch, reason error) error {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to ensure directory exists: %w", err)
		}

		fileName := fmt.Sprintf("%d.csv", batch.Location.StartRow)
		path := filepath.Join(dir, fileName)

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
		log.Infof("Batch %d, starting at byte %d with len %d, has error: %s", batch.Location.StartRow, batch.Location.ByteOffset, batch.Location.ByteLen, reason.Error())

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

// BatchHandlerError fails the process
func BatchHandlerError() BatchErrorHandler {
	return BatchErrorHandler(func(_ batch.Batch, err error) error { return err })
}
