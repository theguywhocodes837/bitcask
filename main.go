package main

import (
	"fmt"
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
