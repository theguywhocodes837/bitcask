package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type Record struct {
	Key       []byte
	Value     []byte
	Timestamp uint32
	Tombstone bool
}

type Store struct {
	filename string
	mu       sync.RWMutex
	file     *os.File
	index    map[string]int64
}

func Open(filename string) (*Store, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file : %w", err)
	}

	store := &Store{
		filename: filename,
		mu:       sync.RWMutex{},
		file:     file,
		index:    make(map[string]int64),
	}

	return store, nil
}

func (s *Store) readRecord(offset int64) (*Record, error) {
	_, err := s.file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	record := &Record{}

	// read timestamp (4 bytes)
	timeStampBuff := make([]byte, 4)
	if _, err := s.file.Read(timeStampBuff); err != nil {
		return nil, err
	}
	record.Timestamp = binary.BigEndian.Uint32(timeStampBuff)

	// read key len (4 bytes)
	keyLenBuff := make([]byte, 4)
	if _, err := s.file.Read(keyLenBuff); err != nil {
		return nil, err
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuff)

	// read value len (4 bytes)
	valueLenBuff := make([]byte, 4)
	if _, err := s.file.Read(valueLenBuff); err != nil {
		return nil, err
	}
	valueLen := binary.BigEndian.Uint32(valueLenBuff)

	// read TombStone (1 byte)
	tombStoneBuff := make([]byte, 1)
	if _, err := s.file.Read(tombStoneBuff); err != nil {
		return nil, err
	}
	record.Tombstone = tombStoneBuff[0] == 1

	// read key data
	key := make([]byte, keyLen)
	if _, err := s.file.Read(key); err != nil {
		return nil, err
	}
	record.Key = key

	// read value data
	value := make([]byte, valueLen)
	if _, err := s.file.Read(value); err != nil {
		return nil, err
	}
	record.Value = value

	return record, nil
}
