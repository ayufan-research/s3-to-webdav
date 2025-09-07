package fs

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpFs struct {
	client   *sftp.Client
	conn     *ssh.Client
	basePath string
	host     string
	port     int
	config   *ssh.ClientConfig
	mu       sync.RWMutex
}

func NewSftpFs(host, username, password string, port int, expectedFingerprint, basePath string) (Fs, error) {
	if port == 0 {
		port = 22
	}

	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			hostFingerprint := keyToFingerprint(key)
			if expectedFingerprint != hostFingerprint {
				return fmt.Errorf("SFTP: Host key fingerprint mismatch. Expected '%s', got '%s'", expectedFingerprint, hostFingerprint)
			}

			log.Printf("SFTP: Host key fingerprint: %s.", hostFingerprint)
			return nil
		},
		Timeout: 5 * time.Second,
	}

	fs := &sftpFs{
		basePath: filepath.Clean(basePath),
		host:     host,
		port:     port,
		config:   config,
	}

	err := fs.reconnect()
	if err != nil {
		return nil, err
	}

	return fs, nil
}

func keyToFingerprint(key ssh.PublicKey) string {
	hash := sha256.Sum256(key.Marshal())
	hexBytes := make([]string, len(hash))
	for i, b := range hash {
		hexBytes[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(hexBytes, ":")
}

func (fs *sftpFs) reconnect() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.client != nil {
		fs.client.Close()
	}
	if fs.conn != nil {
		fs.conn.Close()
	}

	addr := fmt.Sprintf("%s:%d", fs.host, fs.port)
	conn, err := ssh.Dial("tcp", addr, fs.config)
	if err != nil {
		return err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		return err
	}

	fs.client = client
	fs.conn = conn
	log.Printf("SFTP: Connected to %s.", addr)
	return nil
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "network")
}

func (fs *sftpFs) withReconnect(operation func() error) error {
	fs.mu.RLock()
	err := operation()
	fs.mu.RUnlock()

	if err != nil && isConnectionError(err) {
		log.Printf("SFTP: Connection error, attempting to reconnect: %v", err)
		if reconnectErr := fs.reconnect(); reconnectErr != nil {
			return fmt.Errorf("reconnection failed: %v (original error: %v)", reconnectErr, err)
		}
		fs.mu.RLock()
		err = operation()
		fs.mu.RUnlock()
	}
	return err
}

func (fs *sftpFs) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.client != nil {
		fs.client.Close()
	}
	if fs.conn != nil {
		fs.conn.Close()
	}
	return nil
}

func (fs *sftpFs) cleanPath(path string) string {
	return filepath.Join(fs.basePath, filepath.Clean(path))
}

func (fs *sftpFs) ReadDir(path string) ([]os.FileInfo, error) {
	cleanPath := fs.cleanPath(path)
	var result []os.FileInfo
	err := fs.withReconnect(func() error {
		var err error
		result, err = fs.client.ReadDir(cleanPath)
		return err
	})
	return result, err
}

func (fs *sftpFs) Stat(path string) (os.FileInfo, error) {
	cleanPath := fs.cleanPath(path)
	var result os.FileInfo
	err := fs.withReconnect(func() error {
		var err error
		result, err = fs.client.Stat(cleanPath)
		return err
	})
	return result, err
}

func (fs *sftpFs) ReadStream(path string) (io.ReadCloser, error) {
	cleanPath := fs.cleanPath(path)
	var result io.ReadCloser
	err := fs.withReconnect(func() error {
		var err error
		result, err = fs.client.Open(cleanPath)
		return err
	})
	return result, err
}

func (fs *sftpFs) WriteStream(path string, stream io.Reader, contentLength int64, mode os.FileMode) error {
	cleanPath := fs.cleanPath(path)

	return fs.withReconnect(func() error {
		parentDir := filepath.Dir(cleanPath)
		if parentDir != "/" {
			fs.client.MkdirAll(parentDir)
		}

		file, err := fs.client.Create(cleanPath)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(file, stream)
		if err != nil {
			return err
		}

		if mode != 0 {
			return fs.client.Chmod(cleanPath, mode)
		}
		return nil
	})
}

func (fs *sftpFs) Remove(path string) error {
	cleanPath := fs.cleanPath(path)
	return fs.withReconnect(func() error {
		return fs.client.Remove(cleanPath)
	})
}

func (fs *sftpFs) Tree(path string) ([]EntryInfo, error) {
	cleanPath := fs.cleanPath(path)

	var result []EntryInfo
	err := fs.withReconnect(func() error {
		session, err := fs.conn.NewSession()
		if err != nil {
			return err
		}
		defer session.Close()

		cmd := generateTreeCommand(cleanPath)
		output, err := session.Output(cmd)
		if err != nil {
			return err
		}

		result, err = parseTreeOutput(output, cleanPath)
		return err
	})
	return result, err
}
