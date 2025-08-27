package internal

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
	size       int64
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if rw.statusCode == 0 {
		rw.statusCode = 200
	}
	size, err := rw.ResponseWriter.Write(b)
	rw.size += int64(size)
	return size, err
}

func AccessLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap the ResponseWriter to capture status code and response size
		wrapped := &responseWriter{
			ResponseWriter: w,
			statusCode:     200, // Default status code
			size:           0,
		}

		// Call the next handler
		next.ServeHTTP(wrapped, r)

		// Calculate duration
		duration := time.Since(start)

		// Log in Apache Common Log Format with context
		logApacheFormat(r, wrapped.statusCode, wrapped.size, duration)
	})
}

func logApacheFormat(r *http.Request, statusCode int, responseSize int64, duration time.Duration) {
	// Extended Apache Common Log Format:
	// remote_host - remote_user [timestamp] "request_line" status_code request_size/response_size "referer" "user_agent" duration_ms

	// Extract client IP
	remoteHost := getClientIP(r)

	// Get request content length
	requestContentLength := r.ContentLength
	requestSizeStr := "-"
	if requestContentLength >= 0 {
		requestSizeStr = strconv.FormatInt(requestContentLength, 10)
	}

	// Remote user (not available in basic setup, use -)
	remoteUser := "-"

	// Extract user from Authorization header if available
	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(auth, "AWS ") {
			// Extract access key as username
			authParts := strings.SplitN(auth[4:], ":", 2)
			if len(authParts) >= 1 && authParts[0] != "" {
				remoteUser = authParts[0]
			}
		}
	}

	// Format timestamp [day/month/year:hour:minute:second zone]
	timestamp := time.Now().Format("02/Jan/2006:15:04:05 -0700")

	// Request line: "METHOD /path/to/resource HTTP/1.1"
	requestLine := fmt.Sprintf("%s %s %s", r.Method, r.RequestURI, r.Proto)

	// Response size (use - if 0)
	sizeStr := "-"
	if responseSize >= 0 {
		sizeStr = strconv.FormatInt(responseSize, 10)
	}

	// Referer and User-Agent
	referer := r.Header.Get("Referer")
	if referer == "" {
		referer = "-"
	}

	userAgent := r.Header.Get("User-Agent")
	if userAgent == "" {
		userAgent = "-"
	}

	// Get additional context from X-Log header
	contextInfo := ""
	if logInfos := r.Header.Values("X-Log"); len(logInfos) > 0 {
		contextInfo = fmt.Sprintf(" [%s]", strings.Join(logInfos, ", "))
	}

	// Apache Combined Log Format with response time, request size, and context
	logLine := fmt.Sprintf("%s - %s [%s] \"%s\" %d %s/%s \"%s\" \"%s\" %d%s\n",
		remoteHost,
		remoteUser,
		timestamp,
		requestLine,
		statusCode,
		requestSizeStr,
		sizeStr,
		referer,
		userAgent,
		duration.Milliseconds(),
		contextInfo,
	)

	// Write to stdout
	os.Stdout.WriteString(logLine)
}

// SetLogContext sets context information to be included in access logs via X-Log header
func SetLogContext(r *http.Request, context string) {
	r.Header.Set("X-Log", context)
}

func AddLogContext(r *http.Request, context string) {
	r.Header.Add("X-Log", context)
}

func getClientIP(r *http.Request) string {
	// Check for X-Forwarded-For header first (proxy/load balancer)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP from the comma-separated list
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	// Check for X-Real-IP header (reverse proxy)
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}

	// Fall back to RemoteAddr
	ip := r.RemoteAddr

	// Remove port if present
	if colonIndex := strings.LastIndex(ip, ":"); colonIndex != -1 {
		ip = ip[:colonIndex]
	}

	return ip
}
