package tests

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"s3-to-webdav/internal/fs"
)

type FakeWebDAVServer struct {
	files   map[string]*fakeFile
	mu      sync.RWMutex
	server  *httptest.Server
	baseURL string
}

type fakeFile struct {
	content     []byte
	modTime     time.Time
	isDir       bool
	contentType string
}

func NewFakeWebDAVServer() *FakeWebDAVServer {
	f := &FakeWebDAVServer{
		files: make(map[string]*fakeFile),
	}

	handler := http.HandlerFunc(f.handleRequest)
	f.server = httptest.NewServer(handler)
	f.baseURL = f.server.URL

	f.files["/"] = &fakeFile{
		isDir:   true,
		modTime: time.Now(),
	}

	return f
}

func (f *FakeWebDAVServer) Close() {
	f.server.Close()
}

func (f *FakeWebDAVServer) URL() string {
	return f.server.URL
}

func (f *FakeWebDAVServer) CreateWebDAVFs() (fs.Fs, error) {
	return fs.NewWebDAVFs(f.server.URL, "", "", true)
}

func (f *FakeWebDAVServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "PROPFIND":
		f.handlePropFind(w, r)
	case "GET":
		f.handleGet(w, r)
	case "PUT":
		f.handlePut(w, r)
	case "DELETE":
		f.handleDelete(w, r)
	case "MKCOL":
		f.handleMkCol(w, r)
	case "OPTIONS":
		f.handleOptions(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (f *FakeWebDAVServer) handlePropFind(w http.ResponseWriter, r *http.Request) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	filePath := r.URL.Path
	file, exists := f.files[filePath]

	if !exists && strings.HasSuffix(filePath, "/") {
		pathWithoutSlash := strings.TrimSuffix(filePath, "/")
		file, exists = f.files[pathWithoutSlash]
		if exists {
			filePath = pathWithoutSlash
		}
	}

	if !exists {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)

	if file.isDir {
		fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:">`)

		fmt.Fprintf(w, `<d:response>
<d:href>%s</d:href>
<d:propstat>
<d:prop>
<d:resourcetype><d:collection/></d:resourcetype>
<d:getlastmodified>%s</d:getlastmodified>
<d:getcontentlength>0</d:getcontentlength>
</d:prop>
<d:status>HTTP/1.1 200 OK</d:status>
</d:propstat>
</d:response>`, filePath, file.modTime.Format(http.TimeFormat))

		pathPrefix := filePath
		if pathPrefix != "/" && !strings.HasSuffix(pathPrefix, "/") {
			pathPrefix += "/"
		}

		for path, childFile := range f.files {
			if path == filePath {
				continue
			}

			if strings.HasPrefix(path, pathPrefix) {
				relativePath := strings.TrimPrefix(path, pathPrefix)
				if !strings.Contains(relativePath, "/") && relativePath != "" {
					if childFile.isDir {
						fmt.Fprintf(w, `<d:response>
<d:href>%s</d:href>
<d:propstat>
<d:prop>
<d:resourcetype><d:collection/></d:resourcetype>
<d:getlastmodified>%s</d:getlastmodified>
<d:getcontentlength>0</d:getcontentlength>
</d:prop>
<d:status>HTTP/1.1 200 OK</d:status>
</d:propstat>
</d:response>`, path, childFile.modTime.Format(http.TimeFormat))
					} else {
						fmt.Fprintf(w, `<d:response>
<d:href>%s</d:href>
<d:propstat>
<d:prop>
<d:resourcetype/>
<d:getlastmodified>%s</d:getlastmodified>
<d:getcontentlength>%d</d:getcontentlength>
<d:getcontenttype>%s</d:getcontenttype>
</d:prop>
<d:status>HTTP/1.1 200 OK</d:status>
</d:propstat>
</d:response>`, path, childFile.modTime.Format(http.TimeFormat), len(childFile.content), childFile.contentType)
					}
				}
			}
		}

		fmt.Fprintf(w, `</d:multistatus>`)
	} else {
		fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:">
<d:response>
<d:href>%s</d:href>
<d:propstat>
<d:prop>
<d:resourcetype/>
<d:getlastmodified>%s</d:getlastmodified>
<d:getcontentlength>%d</d:getcontentlength>
<d:getcontenttype>%s</d:getcontenttype>
</d:prop>
<d:status>HTTP/1.1 200 OK</d:status>
</d:propstat>
</d:response>
</d:multistatus>`, filePath, file.modTime.Format(http.TimeFormat), len(file.content), file.contentType)
	}
}

func (f *FakeWebDAVServer) handleGet(w http.ResponseWriter, r *http.Request) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	filePath := r.URL.Path
	file, exists := f.files[filePath]
	if !exists || file.isDir {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", file.contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(file.content)))
	w.Header().Set("Last-Modified", file.modTime.Format(http.TimeFormat))
	w.Write(file.content)
}

func (f *FakeWebDAVServer) handlePut(w http.ResponseWriter, r *http.Request) {
	filePath := r.URL.Path
	content, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	dir := path.Dir(filePath)
	f.ensureDir(dir)

	f.files[filePath] = &fakeFile{
		content:     content,
		modTime:     time.Now(),
		isDir:       false,
		contentType: "application/octet-stream",
	}

	w.WriteHeader(http.StatusCreated)
}

func (f *FakeWebDAVServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	filePath := r.URL.Path
	if _, exists := f.files[filePath]; !exists {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	delete(f.files, filePath)
	w.WriteHeader(http.StatusNoContent)
}

func (f *FakeWebDAVServer) handleMkCol(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	filePath := r.URL.Path
	f.files[filePath] = &fakeFile{
		isDir:   true,
		modTime: time.Now(),
	}
	w.WriteHeader(http.StatusCreated)
}

func (f *FakeWebDAVServer) handleOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "OPTIONS, GET, HEAD, POST, PUT, DELETE, TRACE, PROPFIND, PROPPATCH, COPY, MOVE, MKCOL, LOCK, UNLOCK")
	w.Header().Set("DAV", "1, 2")
	w.Header().Set("MS-Author-Via", "DAV")
	w.WriteHeader(http.StatusOK)
}

func (f *FakeWebDAVServer) ensureDir(dirPath string) {
	if dirPath == "/" || dirPath == "." {
		return
	}

	if _, exists := f.files[dirPath]; !exists {
		f.ensureDir(path.Dir(dirPath))
		f.files[dirPath] = &fakeFile{
			isDir:   true,
			modTime: time.Now(),
		}
	}
}

func (f *FakeWebDAVServer) AddFile(filePath string, content []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	dir := path.Dir(filePath)
	f.ensureDir(dir)

	f.files[filePath] = &fakeFile{
		content:     content,
		modTime:     time.Now(),
		isDir:       false,
		contentType: "application/octet-stream",
	}
}
