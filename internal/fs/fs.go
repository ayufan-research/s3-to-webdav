package fs

import (
	"io"
	"os"

	"github.com/studio-b12/gowebdav"
)

type Fs interface {
	Close() error
	ReadDir(path string) ([]os.FileInfo, error)
	Stat(path string) (os.FileInfo, error)
	ReadStream(path string) (io.ReadCloser, error)
	WriteStream(path string, stream io.Reader, contentLength int64, mode os.FileMode) (err error)
	Remove(path string) error
	Tree(path string) ([]EntryInfo, error)
}

func IsNotFound(err error) bool {
	return os.IsNotExist(err) || gowebdav.IsErrNotFound(err)
}
