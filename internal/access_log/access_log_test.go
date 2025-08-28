package access_log

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResponseWriter(t *testing.T) {
	tests := []struct {
		name               string
		writeHeader        bool
		headerCode         int
		writeData          []byte
		expectedStatusCode int
		expectedSize       int64
	}{
		{
			name:               "write header then data",
			writeHeader:        true,
			headerCode:         404,
			writeData:          []byte("not found"),
			expectedStatusCode: 404,
			expectedSize:       9,
		},
		{
			name:               "write data without header",
			writeHeader:        false,
			writeData:          []byte("hello world"),
			expectedStatusCode: 200,
			expectedSize:       11,
		},
		{
			name:               "write header only",
			writeHeader:        true,
			headerCode:         201,
			expectedStatusCode: 201,
			expectedSize:       0,
		},
		{
			name:               "multiple writes",
			writeHeader:        true,
			headerCode:         500,
			writeData:          []byte("error"),
			expectedStatusCode: 500,
			expectedSize:       5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			rw := &responseWriter{
				ResponseWriter: rec,
				statusCode:     200,
				size:           0,
			}

			if tt.writeHeader {
				rw.WriteHeader(tt.headerCode)
			}

			if tt.writeData != nil {
				n, err := rw.Write(tt.writeData)
				require.NoError(t, err)
				assert.Equal(t, len(tt.writeData), n)
			}

			assert.Equal(t, tt.expectedStatusCode, rw.statusCode)
			assert.Equal(t, tt.expectedSize, rw.size)
		})
	}
}

func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		headers    map[string]string
		expectedIP string
	}{
		{
			name:       "X-Forwarded-For single IP",
			remoteAddr: "192.168.1.1:8080",
			headers: map[string]string{
				"X-Forwarded-For": "203.0.113.1",
			},
			expectedIP: "203.0.113.1",
		},
		{
			name:       "X-Forwarded-For multiple IPs",
			remoteAddr: "192.168.1.1:8080",
			headers: map[string]string{
				"X-Forwarded-For": "203.0.113.1, 192.168.1.5, 10.0.0.1",
			},
			expectedIP: "203.0.113.1",
		},
		{
			name:       "X-Forwarded-For with spaces",
			remoteAddr: "192.168.1.1:8080",
			headers: map[string]string{
				"X-Forwarded-For": "  203.0.113.1  ",
			},
			expectedIP: "203.0.113.1",
		},
		{
			name:       "X-Real-IP header",
			remoteAddr: "192.168.1.1:8080",
			headers: map[string]string{
				"X-Real-IP": "203.0.113.2",
			},
			expectedIP: "203.0.113.2",
		},
		{
			name:       "X-Real-IP with spaces",
			remoteAddr: "192.168.1.1:8080",
			headers: map[string]string{
				"X-Real-IP": "  203.0.113.2  ",
			},
			expectedIP: "203.0.113.2",
		},
		{
			name:       "X-Forwarded-For takes precedence over X-Real-IP",
			remoteAddr: "192.168.1.1:8080",
			headers: map[string]string{
				"X-Forwarded-For": "203.0.113.1",
				"X-Real-IP":       "203.0.113.2",
			},
			expectedIP: "203.0.113.1",
		},
		{
			name:       "fallback to RemoteAddr with port",
			remoteAddr: "192.168.1.1:8080",
			headers:    map[string]string{},
			expectedIP: "192.168.1.1",
		},
		{
			name:       "fallback to RemoteAddr without port",
			remoteAddr: "192.168.1.1",
			headers:    map[string]string{},
			expectedIP: "192.168.1.1",
		},
		{
			name:       "IPv6 address with port",
			remoteAddr: "[::1]:8080",
			headers:    map[string]string{},
			expectedIP: "[::1]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.RemoteAddr = tt.remoteAddr

			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			ip := getClientIP(req)
			assert.Equal(t, tt.expectedIP, ip)
		})
	}
}

func TestSetLogContext(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)

	SetLogContext(req, "test-context")

	assert.Equal(t, "test-context", req.Header.Get("X-Log"))
}

func TestAddLogContext(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)

	AddLogContext(req, "context1")
	AddLogContext(req, "context2")

	values := req.Header.Values("X-Log")
	require.Len(t, values, 2)
	assert.Equal(t, "context1", values[0])
	assert.Equal(t, "context2", values[1])
}

func TestSetLogContextOverwrite(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)

	SetLogContext(req, "first-context")
	SetLogContext(req, "second-context")

	assert.Equal(t, "second-context", req.Header.Get("X-Log"))
	values := req.Header.Values("X-Log")
	require.Len(t, values, 1)
}

func TestAccessLogMiddleware(t *testing.T) {
	tests := []struct {
		name              string
		method            string
		path              string
		headers           map[string]string
		handlerStatusCode int
		handlerResponse   string
		expectedInLog     []string
		notExpectedInLog  []string
	}{
		{
			name:              "basic GET request",
			method:            "GET",
			path:              "/test",
			handlerStatusCode: 200,
			handlerResponse:   "hello",
			expectedInLog:     []string{"GET /test HTTP/1.1", "200", "5", "-"},
		},
		{
			name:   "POST request with auth header",
			method: "POST",
			path:   "/upload",
			headers: map[string]string{
				"Authorization": "AWS testkey:signature",
				"User-Agent":    "test-client/1.0",
				"Referer":       "http://example.com",
			},
			handlerStatusCode: 201,
			handlerResponse:   "created",
			expectedInLog:     []string{"POST /upload HTTP/1.1", "201", "testkey", "test-client/1.0", "http://example.com"},
		},
		{
			name:   "request with X-Log context",
			method: "GET",
			path:   "/api",
			headers: map[string]string{
				"X-Log": "operation=sync, bucket=test",
			},
			handlerStatusCode: 404,
			expectedInLog:     []string{"GET /api HTTP/1.1", "404", "[operation=sync, bucket=test]"},
		},
		{
			name:              "request with multiple X-Log headers",
			method:            "PUT",
			path:              "/data",
			handlerStatusCode: 500,
			expectedInLog:     []string{"PUT /data HTTP/1.1", "500"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.name == "request with multiple X-Log headers" {
					AddLogContext(r, "context1")
					AddLogContext(r, "context2")
				}
				w.WriteHeader(tt.handlerStatusCode)
				if tt.handlerResponse != "" {
					w.Write([]byte(tt.handlerResponse))
				}
			})

			middleware := AccessLogMiddleware(handler)

			req := httptest.NewRequest(tt.method, tt.path, nil)
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			rec := httptest.NewRecorder()
			middleware.ServeHTTP(rec, req)

			w.Close()
			os.Stdout = oldStdout

			var buf bytes.Buffer
			io.Copy(&buf, r)
			logOutput := buf.String()

			for _, expected := range tt.expectedInLog {
				assert.Contains(t, logOutput, expected, "Expected '%s' in log output", expected)
			}

			for _, notExpected := range tt.notExpectedInLog {
				assert.NotContains(t, logOutput, notExpected, "Did not expect '%s' in log output", notExpected)
			}

			if tt.name == "request with multiple X-Log headers" {
				assert.Contains(t, logOutput, "[context1, context2]")
			}

			assert.Equal(t, tt.handlerStatusCode, rec.Code)
			if tt.handlerResponse != "" {
				assert.Equal(t, tt.handlerResponse, rec.Body.String())
			}
		})
	}
}
