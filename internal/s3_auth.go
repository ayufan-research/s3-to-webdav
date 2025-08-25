package internal

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// S3AuthConfig holds the configuration for S3 authentication
type S3AuthConfig struct {
	AccessKey string
	SecretKey string
}

// S3AuthMiddleware provides AWS-style authentication including presigned URLs
func S3AuthMiddleware(config S3AuthConfig, next http.Handler) http.Handler {
	// Skip authentication if no access key is configured
	if config.AccessKey == "" {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if validatePresignedURL(r, config) {
			AddLogContext(r, "presigned")
			next.ServeHTTP(w, r)
			return
		}

		if validateAuthorized(r, config) {
			AddLogContext(r, "auth")
			next.ServeHTTP(w, r)
			return
		}

		AddLogContext(r, "auth-fail")
		w.Header().Set("WWW-Authenticate", "AWS")
		http.Error(w, "Authorization failed", http.StatusUnauthorized)
	})
}

// calculateSignature calculates AWS v2 signature from the request and date
func calculateSignature(r *http.Request, date, secretKey string) string {
	method := r.Method
	contentMD5 := r.Header.Get("Content-MD5")
	contentType := r.Header.Get("Content-Type")
	canonicalizedResource := r.URL.Path

	stringToSign := method + "\n" + contentMD5 + "\n" + contentType + "\n" + date + "\n" + canonicalizedResource

	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// validateAuthorized validates AWS-style Authorization header including parsing and signature validation
func validateAuthorized(r *http.Request, config S3AuthConfig) bool {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return false
	}

	// Check AWS format: "AWS AccessKeyId:Signature"
	if !strings.HasPrefix(authHeader, "AWS ") {
		return false
	}

	// Extract access key and signature
	authParts := strings.SplitN(authHeader[4:], ":", 2)
	if len(authParts) != 2 || authParts[0] != config.AccessKey {
		return false
	}

	// Validate the signature
	date := r.Header.Get("Date")
	expectedSignature := calculateSignature(r, date, config.SecretKey)
	return expectedSignature == authParts[1]
}

// validatePresignedURL validates AWS-style presigned URL signatures
func validatePresignedURL(r *http.Request, config S3AuthConfig) bool {
	query := r.URL.Query()

	// Check for required presigned URL parameters
	accessKey := query.Get("AWSAccessKeyId")
	signature := query.Get("Signature")
	expires := query.Get("Expires")

	if accessKey == "" || signature == "" || expires == "" {
		return false
	}

	// Validate access key
	if accessKey != config.AccessKey {
		return false
	}

	// Check expiration
	expiresTime, err := strconv.ParseInt(expires, 10, 64)
	if err != nil {
		return false
	}

	if time.Now().Unix() > expiresTime {
		return false
	}

	// Calculate expected signature using shared function
	expectedSignature := calculateSignature(r, expires, config.SecretKey)

	// URL decode the provided signature
	decodedSignature, err := url.QueryUnescape(signature)
	if err != nil {
		return false
	}

	return expectedSignature == decodedSignature
}
