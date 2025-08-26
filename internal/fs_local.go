package internal

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

func (fs *localFs) WriteStream(path string, stream io.Reader, mode os.FileMode) error {
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
