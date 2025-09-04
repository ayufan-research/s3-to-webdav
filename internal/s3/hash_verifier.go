package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
)

var ErrBadDigest = errors.New("BadDigest")

type hashVerifier struct {
	reader      io.Reader
	expectedHex string
	hasher      hash.Hash
}

func newHashVerifier(reader io.Reader, hash hash.Hash, expectedHex string) io.Reader {
	hasher := sha256.New()
	return &hashVerifier{
		reader:      io.TeeReader(reader, hasher),
		expectedHex: expectedHex,
		hasher:      hasher,
	}
}

func (s *hashVerifier) Read(p []byte) (int, error) {
	n, err := s.reader.Read(p)

	// If we hit EOF, verify the hash before returning
	if err == io.EOF {
		actualHash := hex.EncodeToString(s.hasher.Sum(nil))
		if actualHash != s.expectedHex {
			return n, ErrBadDigest
		}
	}

	return n, err
}
