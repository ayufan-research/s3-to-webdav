package internal

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
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
		if validatePresignedURLV2(r, config) {
			AddLogContext(r, "presigned-v2")
		} else if validatePresignedURLV4(r, config) {
			AddLogContext(r, "presigned-v4")
		} else if validateAuthorizationV2(r, config) {
			AddLogContext(r, "auth-v2")
		} else if validateAuthorizationV4(r, config) {
			AddLogContext(r, "auth-v4")
		} else {
			AddLogContext(r, "auth-fail")
			w.Header().Set("WWW-Authenticate", "AWS")
			http.Error(w, "Authorization failed", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// calculateSignature calculates AWS v2 signature from the request and date
func calculateSignature(r *http.Request, date, secretKey string) string {
	method := r.Method
	contentMD5 := r.Header.Get("Content-MD5")
	contentType := r.Header.Get("Content-Type")
	canonicalizedResource := r.URL.Path
	if canonicalizedResource == "" {
		canonicalizedResource = "/"
	}

	// For v2, add query parameters that are part of the sub-resource
	if query := r.URL.RawQuery; query != "" {
		// Only include specific query parameters in v2 signature
		canonicalizedResource += "?" + query
	}

	stringToSign := method + "\n" + contentMD5 + "\n" + contentType + "\n" + date + "\n" + canonicalizedResource

	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// validateAuthorizationV2 validates AWS-style Authorization header including parsing and signature validation
func validateAuthorizationV2(r *http.Request, config S3AuthConfig) bool {
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

// validatePresignedURLV2 validates AWS-style presigned URL signatures
func validatePresignedURLV2(r *http.Request, config S3AuthConfig) bool {
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

// AWS Signature Version 4 implementation

// calculateSignatureV4 calculates AWS v4 signature
func calculateSignatureV4(r *http.Request, region, service, secretKey, date, signedHeaders string) (string, error) {
	// Step 1: Create canonical request
	canonicalRequest, err := createCanonicalRequest(r, signedHeaders)
	if err != nil {
		return "", err
	}

	// Step 2: Create string to sign
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", date[:8], region, service)

	hasher := sha256.New()
	hasher.Write([]byte(canonicalRequest))
	hashedCanonicalRequest := hex.EncodeToString(hasher.Sum(nil))

	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s", algorithm, date, credentialScope, hashedCanonicalRequest)

	// Step 3: Calculate signature
	kDate := hmacSHA256([]byte("AWS4"+secretKey), date[:8])
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hmacSHA256(kSigning, stringToSign)

	return hex.EncodeToString(signature), nil
}

// createCanonicalRequest creates the canonical request for AWS v4 signature
func createCanonicalRequest(r *http.Request, signedHeaders string) (string, error) {
	// HTTP Method
	method := r.Method

	// Canonical URI - must be URL-encoded per AWS v4 spec
	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	} else {
		// AWS v4 requires URI path segments to be URL-encoded
		canonicalURI = canonicalizeURI(canonicalURI)
	}

	// Canonical Query String
	canonicalQueryString := createCanonicalQueryString(r.URL.Query())

	// Canonical Headers
	canonicalHeaders, err := createCanonicalHeaders(r, signedHeaders)
	if err != nil {
		return "", err
	}

	// Payload hash
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method, canonicalURI, canonicalQueryString, canonicalHeaders, signedHeaders, payloadHash)

	return canonicalRequest, nil
}

// createCanonicalQueryString creates canonical query string for AWS v4
func createCanonicalQueryString(values url.Values) string {
	var parts []string
	for key, vals := range values {
		for _, val := range vals {
			// AWS v4 requires specific encoding for query parameters
			encodedKey := url.QueryEscape(key)
			encodedKey = strings.ReplaceAll(encodedKey, "+", "%20")

			encodedVal := url.QueryEscape(val)
			encodedVal = strings.ReplaceAll(encodedVal, "+", "%20")

			parts = append(parts, fmt.Sprintf("%s=%s", encodedKey, encodedVal))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

// createCanonicalHeaders creates canonical headers string
func createCanonicalHeaders(r *http.Request, signedHeaders string) (string, error) {
	headers := make(map[string]string)

	headerNames := strings.Split(signedHeaders, ";")
	for _, headerName := range headerNames {
		headerName = strings.ToLower(strings.TrimSpace(headerName))
		headerValue := r.Header.Get(headerName)
		if headerValue == "" && headerName == "host" {
			headerValue = r.Host
		}
		headers[headerName] = strings.TrimSpace(headerValue)
	}

	var canonicalHeaders string
	for _, headerName := range headerNames {
		headerName = strings.ToLower(strings.TrimSpace(headerName))
		canonicalHeaders += fmt.Sprintf("%s:%s\n", headerName, headers[headerName])
	}

	return canonicalHeaders, nil
}

// canonicalizeURI encodes URI path according to AWS v4 specification
func canonicalizeURI(path string) string {
	// Handle empty path
	if path == "" || path == "/" {
		return "/"
	}

	// Split path into segments and encode each segment
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if segment != "" {
			// AWS v4 URI encoding: encode everything except unreserved characters
			// Unreserved characters: A-Z a-z 0-9 - . _ ~
			encoded := awsURIEscape(segment)
			segments[i] = encoded
		}
	}

	canonicalPath := strings.Join(segments, "/")

	// Ensure path starts with /
	if !strings.HasPrefix(canonicalPath, "/") {
		canonicalPath = "/" + canonicalPath
	}

	// Remove duplicate slashes but preserve trailing slash if original had it
	for strings.Contains(canonicalPath, "//") {
		canonicalPath = strings.ReplaceAll(canonicalPath, "//", "/")
	}

	return canonicalPath
}

// awsURIEscape performs AWS-compliant URI encoding
func awsURIEscape(s string) string {
	encoded := ""
	for _, b := range []byte(s) {
		c := string(b)
		if isUnreserved(b) {
			encoded += c
		} else {
			encoded += fmt.Sprintf("%%%02X", b)
		}
	}
	return encoded
}

// isUnreserved checks if byte is an unreserved character per AWS v4 spec
func isUnreserved(b byte) bool {
	return (b >= 'A' && b <= 'Z') ||
		(b >= 'a' && b <= 'z') ||
		(b >= '0' && b <= '9') ||
		b == '-' || b == '.' || b == '_' || b == '~'
}

// hmacSHA256 performs HMAC-SHA256
func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// validateAuthorizationV4 validates AWS v4 Authorization header
func validateAuthorizationV4(r *http.Request, config S3AuthConfig) bool {
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return false
	}

	// Parse the authorization header
	authParts := strings.Split(authHeader[17:], ",") // Remove "AWS4-HMAC-SHA256 "
	authData := make(map[string]string)

	for _, part := range authParts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			authData[kv[0]] = kv[1]
		}
	}

	credential := authData["Credential"]
	signature := authData["Signature"]
	signedHeaders := authData["SignedHeaders"]

	if credential == "" || signature == "" || signedHeaders == "" {
		return false
	}

	// Parse credential
	credentialParts := strings.Split(credential, "/")
	if len(credentialParts) < 5 {
		return false
	}

	accessKey := credentialParts[0]
	region := credentialParts[2]
	service := credentialParts[3]

	// Validate access key
	if accessKey != config.AccessKey {
		return false
	}

	// Get the date from X-Amz-Date header
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return false
	}

	// Calculate expected signature
	expectedSignature, err := calculateSignatureV4(r, region, service, config.SecretKey, amzDate, signedHeaders)
	if err != nil {
		return false
	}

	return expectedSignature == signature
}

// validatePresignedURLV4 validates AWS v4 presigned URLs
func validatePresignedURLV4(r *http.Request, config S3AuthConfig) bool {
	query := r.URL.Query()

	// Check for v4 presigned URL parameters
	credential := query.Get("X-Amz-Credential")
	signature := query.Get("X-Amz-Signature")
	signedHeaders := query.Get("X-Amz-SignedHeaders")
	expires := query.Get("X-Amz-Expires")
	date := query.Get("X-Amz-Date")

	if credential == "" || signature == "" || signedHeaders == "" || expires == "" || date == "" {
		return false
	}

	// Parse credential
	credentialParts := strings.Split(credential, "/")
	if len(credentialParts) < 5 {
		return false
	}

	accessKey := credentialParts[0]
	region := credentialParts[2]
	service := credentialParts[3]

	// Validate access key
	if accessKey != config.AccessKey {
		return false
	}

	// Check expiration
	expiresSeconds, err := strconv.ParseInt(expires, 10, 64)
	if err != nil {
		return false
	}

	// Parse date and check if expired
	requestTime, err := time.Parse("20060102T150405Z", date)
	if err != nil {
		return false
	}

	if time.Now().After(requestTime.Add(time.Duration(expiresSeconds) * time.Second)) {
		return false
	}

	// For presigned URLs, we need to create a modified request without the signature parameter
	modifiedQuery := r.URL.Query()
	modifiedQuery.Del("X-Amz-Signature")

	// Create a copy of the request for signature calculation
	modifiedURL := *r.URL
	modifiedURL.RawQuery = modifiedQuery.Encode()
	modifiedRequest := *r
	modifiedRequest.URL = &modifiedURL

	// Calculate expected signature
	expectedSignature, err := calculateSignatureV4(&modifiedRequest, region, service, config.SecretKey, date, signedHeaders)
	if err != nil {
		return false
	}

	return expectedSignature == signature
}
