package internal

import (
	"crypto/tls"
	"log"
	"net/http"

	"github.com/studio-b12/gowebdav"
)

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

	return client, nil
}
