package internal

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// getOrCreateCertificates gets existing certificates from directory or creates new ones
func GetOrCreateCertificates(certDir string) (string, string, error) {
	certPath := filepath.Join(certDir, "cert.pem")
	keyPath := filepath.Join(certDir, "key.pem")

	// Check if certificates already exist and are valid
	if _, err := os.Stat(certPath); err == nil {
		if _, err := os.Stat(keyPath); err == nil {
			log.Printf("TLS: Found existing certificates in %s", certDir)
			return certPath, keyPath, nil
		}
	}

	log.Printf("TLS: Generating new self-signed certificates in %s", certDir)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(certDir, 0755); err != nil {
		return "", "", fmt.Errorf("failed to create certificate directory: %v", err)
	}

	// Generate certificates
	certPEM, keyPEM, err := generateSelfSignedCertPEM()
	if err != nil {
		return "", "", fmt.Errorf("failed to generate certificates: %v", err)
	}

	// Write certificate file
	if err := ioutil.WriteFile(certPath, certPEM, 0644); err != nil {
		return "", "", fmt.Errorf("failed to write certificate file: %v", err)
	}

	// Write key file
	if err := ioutil.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return "", "", fmt.Errorf("failed to write key file: %v", err)
	}

	log.Printf("TLS: Generated new certificates: %s, %s", certPath, keyPath)
	return certPath, keyPath, nil
}

// generateSelfSignedCertPEM generates a self-signed TLS certificate and returns PEM data
func generateSelfSignedCertPEM() ([]byte, []byte, error) {
	// Generate private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %v", err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:       []string{"S3-to-WebDAV Bridge"},
			OrganizationalUnit: []string{"Self-Signed Certificate"},
			Country:            []string{"US"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour), // Valid for 10 years
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Add common names and IPs
	template.DNSNames = []string{
		"localhost",
		"s3-to-webdav",
		"*.s3-to-webdav",
	}
	template.IPAddresses = []net.IP{
		net.IPv4(127, 0, 0, 1),
		net.IPv6loopback,
		net.IPv4zero,
		net.IPv6zero,
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %v", err)
	}

	// Encode certificate to PEM
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	// Encode private key to PEM
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	log.Printf("TLS: Self-signed certificate generated successfully")
	log.Printf("TLS: Certificate valid for: localhost, s3-to-webdav, 127.0.0.1, ::1")
	log.Printf("TLS: Certificate expires: %s", template.NotAfter.Format(time.RFC3339))

	return certPEM, keyPEM, nil
}

// GetCertificateFingerprint calculates and returns the SHA256 fingerprint of a certificate file
// in the format compatible with Proxmox (xx:xx:xx:xx...)
func GetCertificateFingerprint(certPath string) (string, error) {
	// Read certificate file
	certData, err := ioutil.ReadFile(certPath)
	if err != nil {
		return "", fmt.Errorf("failed to read certificate file: %v", err)
	}

	// Parse PEM block
	block, _ := pem.Decode(certData)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", fmt.Errorf("invalid certificate format")
	}

	// Parse certificate
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("failed to parse certificate: %v", err)
	}

	// Calculate SHA256 fingerprint
	fingerprint := sha256.Sum256(cert.Raw)

	// Format as colon-separated hex pairs (compatible with Proxmox)
	hexString := hex.EncodeToString(fingerprint[:])
	var parts []string
	for i := 0; i < len(hexString); i += 2 {
		parts = append(parts, strings.ToUpper(hexString[i:i+2]))
	}

	return strings.Join(parts, ":"), nil
}

func GetOrCreateRandomSecret(file string, length int) (string, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return "", err
	}

	if data, err := os.ReadFile(file); err == nil {
		return strings.TrimSpace(string(data)), nil
	}

	// Generate random secret
	secret, err := generateRandomKey(length)
	if err != nil {
		return "", err
	}

	// Write secret to file
	if err := ioutil.WriteFile(file, []byte(secret), 0600); err != nil {
		return "", fmt.Errorf("failed to write secret file: %v", err)
	}

	return secret, nil
}

// generateRandomKey generates a random key of specified length
func generateRandomKey(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
