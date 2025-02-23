package main

import (
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
