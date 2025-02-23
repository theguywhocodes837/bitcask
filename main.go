package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const HEADER_SIZE = 13

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

func (s *Store) writeRecord(record *Record) (int64, error) {
	totalSize := HEADER_SIZE + len(record.Key) + len(record.Value)
	buf := make([]byte, totalSize)

	binary.BigEndian.PutUint32(buf[:4], record.Timestamp)

	binary.BigEndian.PutUint32(buf[:4], uint32(len(record.Key)))

	binary.BigEndian.PutUint32(buf[:4], uint32(len(record.Value)))

	if record.Tombstone {
		buf[12] = 1
	} else {
		buf[12] = 0
	}

	copy(buf[HEADER_SIZE:HEADER_SIZE+len(record.Key)], record.Key)
	copy(buf[HEADER_SIZE+len(record.Key):], record.Value)

	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	if _, err := s.file.Write(buf); err != nil {
		return 0, err
	}

	return offset, nil
}

func (s *Store) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key can not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record := &Record{
		Key:       key,
		Value:     value,
		Timestamp: uint32(time.Now().Unix()),
	}

	offset, err := s.writeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	s.index[string(key)] = offset
	return nil
}
