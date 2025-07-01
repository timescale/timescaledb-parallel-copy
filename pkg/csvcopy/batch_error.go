package csvcopy

// BatchHandlerLog prints a log line that reports the error in the given batch
func BatchHandlerLog(log Logger, next BatchErrorHandler) BatchErrorHandler {
	return BatchErrorHandler(func(batch Batch, reason error) *BatchError {
		log.Infof("Batch %d, starting at byte %d with len %d, has error: %s", batch.Location.StartRow, batch.Location.ByteOffset, batch.Location.ByteLen, reason.Error())

		if next != nil {
			return next(batch, reason)
		}
		return NewErrContinue(reason)
	})
}

// BatchHandlerNoop no operation
func BatchHandlerNoop() BatchErrorHandler {
	return BatchErrorHandler(func(_ Batch, reason error) *BatchError { return NewErrContinue(reason) })
}

// BatchHandlerError fails the process
func BatchHandlerError() BatchErrorHandler {
	return BatchErrorHandler(func(_ Batch, reason error) *BatchError { return NewErrStop(reason) })
}
