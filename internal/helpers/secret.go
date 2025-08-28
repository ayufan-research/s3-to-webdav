package helpers

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

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
