package fs

import (
	"crypto/tls"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/studio-b12/gowebdav"
)

type webdavFs struct {
	client *gowebdav.Client
}

func NewWebDAVFs(webdavURL, webdavUser, webdavPassword string, webdavInsecure bool) (Fs, error) {
	// Create WebDAV client
	log.Printf("WebDAV: URL: %s", webdavURL)
	log.Printf("WebDAV: User: %s", webdavUser)

	client := gowebdav.NewClient(webdavURL, webdavUser, webdavPassword)

	// Configure TLS settings if needed
	if webdavInsecure {
		log.Printf("WebDAV: Allowing self-signed certificates")
		client.SetTransport(&http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		})
	}

	if err := client.Connect(); err != nil {
		return nil, err
	}
	log.Printf("WebDAV: Successfully connected to WebDAV server")

	return &webdavFs{client: client}, nil
}

func (fs *webdavFs) ReadDir(path string) ([]os.FileInfo, error) {
	return fs.client.ReadDir(path)
}

func (fs *webdavFs) Stat(path string) (os.FileInfo, error) {
	return fs.client.Stat(path)
}

func (fs *webdavFs) ReadStream(path string) (io.ReadCloser, error) {
	return fs.client.ReadStream(path)
}

func (fs *webdavFs) WriteStream(path string, stream io.Reader, contentLength int64, mode os.FileMode) error {
	return fs.client.WriteStreamWithLength(path, stream, contentLength, mode)
}

func (fs *webdavFs) Remove(path string) error {
	return fs.client.Remove(path)
}
