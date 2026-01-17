package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file, path: filePath}, nil
}

func (w *WAL) Write(p []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(p)))

	if _, err := w.file.Write(lenBuf); err != nil {
		return err
	}

	if _, err := w.file.Write(p); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Rewrite(records [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file for rewrite: %w", err)
	}

	if err := os.Truncate(w.path, 0); err != nil {
		return fmt.Errorf("failed to truncate WAL file: %w", err)
	}

	file, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL file for rewrite: %w", err)
	}
	w.file = file

	lenBuf := make([]byte, 4)
	for _, record := range records {
		binary.BigEndian.PutUint32(lenBuf, uint32(len(record)))
		if _, err := w.file.Write(lenBuf); err != nil {
			log.Printf("ERROR: Failed to write record length during WAL rewrite: %v", err)
			continue
		}
		if _, err := w.file.Write(record); err != nil {
			log.Printf("ERROR: Failed to write record data during WAL rewrite: %v", err)
			continue
		}
	}

	return w.file.Sync()
}


func (w *WAL) Close() error {
	return w.file.Close()
}

func ReadAll(filePath string) ([][]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No WAL file yet, which is fine
		}
		return nil, err
	}
	defer file.Close()

	var records [][]byte
	lenBuf := make([]byte, 4)

	for {
		_, err := io.ReadFull(file, lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		length := binary.BigEndian.Uint32(lenBuf)
		record := make([]byte, length)
		if _, err := io.ReadFull(file, record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return records, nil
}