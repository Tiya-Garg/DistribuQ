package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Header size: 4 bytes length + 4 bytes CRC32 checksum.
const headerSize = 8

var (
	ErrCorruptRecord = errors.New("wal: corrupt record detected (CRC mismatch)")
	ErrClosed        = errors.New("wal: log is closed")
)

// Entry represents a binary record stored in the Write-Ahead Log.
type Entry struct {
	Index uint64
	Data  []byte
}

// Log represents a high-performance, append-only Write-Ahead Log.
type Log struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// Open opens or creates an append-only WAL file at the given path.
func Open(path string) (*Log, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create directory %s: %w", dir, err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open file %s: %w", path, err)
	}

	return &Log{
		file: f,
		path: path,
	}, nil
}

// Append appends a payload to the WAL, calculating its CRC32 checksum and fsyncing to disk.
func (l *Log) Append(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return ErrClosed
	}

	length := uint32(len(data))
	checksum := crc32.ChecksumIEEE(data)

	buf := make([]byte, headerSize+len(data))
	binary.BigEndian.PutUint32(buf[0:4], length)
	binary.BigEndian.PutUint32(buf[4:8], checksum)
	copy(buf[8:], data)

	if _, err := l.file.Write(buf); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}

	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}

	return nil
}

// ReadAll iterates over all valid records sequentially from the start of the WAL file.
func (l *Log) ReadAll() ([][]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil, ErrClosed
	}

	// Seek to beginning to read all entries
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal: seek start: %w", err)
	}

	var records [][]byte
	headerBuf := make([]byte, headerSize)

	for {
		_, err := io.ReadFull(l.file, headerBuf)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("wal: read header: %w", err)
		}

		length := binary.BigEndian.Uint32(headerBuf[0:4])
		expectedChecksum := binary.BigEndian.Uint32(headerBuf[4:8])

		data := make([]byte, length)
		if _, err := io.ReadFull(l.file, data); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Torn write at the end of file
				break
			}
			return nil, fmt.Errorf("wal: read data: %w", err)
		}

		if crc32.ChecksumIEEE(data) != expectedChecksum {
			return nil, ErrCorruptRecord
		}

		records = append(records, data)
	}

	// Seek back to end for future appends
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("wal: seek end: %w", err)
	}

	return records, nil
}

// Truncate clears the WAL and starts fresh.
func (l *Log) Truncate() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return ErrClosed
	}

	if err := l.file.Close(); err != nil {
		return fmt.Errorf("wal: close for truncate: %w", err)
	}

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: truncate open: %w", err)
	}
	l.file = f
	return l.file.Sync()
}

// Close flushes and closes the WAL.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}
