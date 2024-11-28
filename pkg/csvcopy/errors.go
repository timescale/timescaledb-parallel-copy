package csvcopy

import (
	"fmt"
)

type MultiError []BatchError

func (e MultiError) Error() string {
	return fmt.Sprintf("found %d errors. first one is %s", len(e), e[0].Error())
}

type BatchError struct {
	Err  error
	Path string
}

func (e BatchError) Error() string {
	return fmt.Sprintf("Batch error stored at %s, reason %s", e.Path, e.Err.Error())
}

func (e BatchError) Unwrap() error { return e.Err }
