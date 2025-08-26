package internal

import (
	"io"
	"os"
)

type Fs interface {
	ReadDir(path string) ([]os.FileInfo, error)
	Stat(path string) (os.FileInfo, error)
	ReadStream(path string) (io.ReadCloser, error)
	WriteStream(path string, stream io.Reader, _ os.FileMode) (err error)
	Remove(path string) error
}
