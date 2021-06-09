package draft

import (
	"errors"
	"fmt"
)

var (
	ErrDupStart     = errModuleStart("duplicated start")
	ErrUnchangeable = errModule("change the status")

	ErrReaderNotExist    = errModule("reader not exist")
	ErrWriterNotExist    = errModule("writer not exist")
	ErrCollectorNotExist = errModule("collector not exist")

	ErrImfDenied    = errModuleStart("denied")
	ErrImfOpTimeout = errImfModuleStatus("imf operation timeouts")
)

func errModuleStart(text string) error {
	return errors.New("module failed to start: " + text)
}

func errModule(text string) error {
	return errors.New("module failed to comply: " + text)
}

func errImfModuleStart(text string) error {
	return errors.New("imf module failed to start: " + text)
}

func errImfModuleStatus(text string) error {
	return errors.New("imf module status error: " + text)
}

func ErrImfModuleInternal(err error) error {
	return fmt.Errorf("an internal error occurs in imf module: %w", err)
}
