package draft

import "errors"

var (
	ErrReaderNotExist = errWriterNotExist()
	ErrWriterNotExist = errWriterNotExist()
	ErrAsyncTimeout   = errAsyncTimeout()
)

func errReaderNotExist() error {
	return errors.New("reader does not exist")
}

func errWriterNotExist() error {
	return errors.New("writer does not exist")
}

func errAsyncTimeout() error {
	return errors.New("async primitive timeouts")
}
