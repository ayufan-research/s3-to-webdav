package fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type localFs struct {
	rootPath string
}

func NewLocalFs(rootPath string) (Fs, error) {
	absPath, err := filepath.Abs(rootPath)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(absPath, 0755); err != nil {
		return nil, err
	}

	return &localFs{
		rootPath: absPath,
	}, nil
}

func (fs *localFs) Close() error {
	return nil
}

func (fs *localFs) getFullPath(path string) (string, error) {
	fullPath := filepath.Join(fs.rootPath, filepath.Clean(path))

	// Check that the resolved path is still within rootPath
	rel, err := filepath.Rel(fs.rootPath, fullPath)
	if err != nil {
		return "", err
	}

	// If the relative path starts with "..", it's trying to escape
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path escapes root directory: %s", path)
	}

	return fullPath, nil
}

func (fs *localFs) ReadDir(path string) ([]os.FileInfo, error) {
	fullPath, err := fs.getFullPath(path)
	if err != nil {
		return nil, err
	}
	dirInfos, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}
	var fileInfos []os.FileInfo
	for _, dirInfo := range dirInfos {
		fileInfo, err := dirInfo.Info()
		if err != nil {
			return nil, err
		}
		fileInfos = append(fileInfos, fileInfo)
	}
	return fileInfos, nil
}

func (fs *localFs) Stat(path string) (os.FileInfo, error) {
	fullPath, err := fs.getFullPath(path)
	if err != nil {
		return nil, err
	}
	return os.Stat(fullPath)
}

func (fs *localFs) ReadStream(path string) (io.ReadCloser, error) {
	fullPath, err := fs.getFullPath(path)
	if err != nil {
		return nil, err
	}
	return os.Open(fullPath)
}

func (fs *localFs) WriteStream(path string, stream io.Reader, contentLength int64, mode os.FileMode) error {
	fullPath, err := fs.getFullPath(path)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(filepath.Dir(fullPath), filepath.Base(fullPath)+".tmp")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()

	defer func() {
		tempFile.Close()
		os.Remove(tempPath)
	}()

	if _, err := io.Copy(tempFile, stream); err != nil {
		return err
	}

	if err := tempFile.Chmod(mode); err != nil {
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	return os.Rename(tempPath, fullPath)
}

func (fs *localFs) Remove(path string) error {
	fullPath, err := fs.getFullPath(path)
	if err != nil {
		return err
	}
	return os.Remove(fullPath)
}

func (fs *localFs) Tree(path string) ([]EntryInfo, error) {
	var entries []EntryInfo

	err := fs.treeWalk(path, func(relativePath string, info os.FileInfo) error {
		entries = append(entries, EntryInfo{
			Path:         relativePath,
			Size:         info.Size(),
			LastModified: info.ModTime().Unix(),
			IsDir:        info.IsDir(),
		})
		return nil
	})

	return entries, err
}

func (fs *localFs) treeWalk(path string, fn func(string, os.FileInfo) error) error {
	fullPath, err := fs.getFullPath(path)
	if err != nil {
		return err
	}

	return filepath.Walk(fullPath, func(walkPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relativePath, err := filepath.Rel(fs.rootPath, walkPath)
		if err != nil {
			return err
		}

		if relativePath == "." {
			relativePath = ""
		}

		return fn(relativePath, info)
	})
}
