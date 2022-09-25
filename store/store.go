package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tidwall/match"
)

type Store struct {
	StoreIndex *StoreIndex
	readerPool *ReaderPool
}

var ErrKeyNotFound = errors.New("key not found")

func (s *Store) GetLen() int {
	return len(s.StoreIndex.SortedKeys)
}

func (s *Store) GetRecordIndex(key string) (*IndexRecord, error) {
	indexRecord, ok := s.StoreIndex.Index[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return &indexRecord, nil
}

type ErrReadingRecordFromDisk struct {
	Err error
}

func (e ErrReadingRecordFromDisk) Error() string {
	return fmt.Sprintf("error reading record from disk: %s", e.Err.Error())
}

func (hr *Store) ScanFields(start int, count int, pattern string) (records []IndexRecord, cursor int, err error) {
	if start < 0 {
		return nil, 0, fmt.Errorf("start must be >= 0")
	}
	/*
		// find the first key that greater than or equal to from with
		// binary search in the hr.StoreIndex.Keys
		// if from is nil, then start from the beginning
		if from != nil {
			start = sort.Search(len(hr.StoreIndex.SortedKeys), func(i int) bool {
				return hr.StoreIndex.SortedKeys[i] >= *from
			})
			if start == len(hr.StoreIndex.SortedKeys) {
				return []IndexRecord{}, nil // Finished scanning
			}
		}
	*/
	var i = start
	for ; i < len(hr.StoreIndex.SortedKeys); i++ {
		key := hr.StoreIndex.SortedKeys[i]

		if pattern != "" && !match.Match(key, pattern) {
			continue
		}

		indexRecord, ok := hr.StoreIndex.Index[key]
		if !ok {
			return nil, 0, ErrKeyNotFound
		}

		records = append(records, indexRecord)

		if len(records) >= count {
			break
		}
	}
	cursor = i + 1
	if cursor > len(hr.StoreIndex.SortedKeys)-1 {
		cursor = 0
	}
	return records, cursor, nil
}

func (s *Store) GetRecord(key string) (record *Record, err error) {
	// find record in s.StoreIndex first
	indexRecord, ok := s.StoreIndex.Index[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// get reader from pool
	reader, err := s.readerPool.GetReader()
	if err != nil {
		return nil, err
	}
	defer s.readerPool.ReturnReader(reader)

	_, err = reader.Seek(indexRecord.Offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	recordBytes := make([]byte, indexRecord.Len)
	bytesRead, err := reader.Read(recordBytes)
	if err != nil {
		return nil, ErrReadingRecordFromDisk{err}
	}

	if bytesRead != indexRecord.Len {
		return nil, ErrReadingRecordFromDisk{errors.New("not enough bytes read")}
	}

	record = &Record{}
	err = json.Unmarshal(recordBytes, record)
	if err != nil {
		return nil, ErrReadingRecordFromDisk{err}
	}

	return record, nil

}

type Config struct {
	MaxConnections      int
	DefaultTimeout      time.Duration
	DrainTimeout        time.Duration
	KeysDontNeedSorting bool
}

// Opens a store w/o an index
func NewStoreFromRecordsWithConfig(openReaderSeekCloser OpenReaderSeekCloser, config Config) (store *Store, err error) {
	store = &Store{}

	recordReader, err := openReaderSeekCloser()
	if err != nil {
		return nil, fmt.Errorf("error opening record reader %w", err)
	}
	defer recordReader.Close()

	storeIndex, err := BuildIndex(recordReader)
	if err != nil {
		return nil, fmt.Errorf("error building index %w", err)
	}
	store.StoreIndex = storeIndex

	store.readerPool, err = NewReaderPoolAdvanced(openReaderSeekCloser, config.MaxConnections, config.DefaultTimeout, config.DrainTimeout)
	if err != nil {
		return nil, err
	}

	return store, nil
}

func NewEmptyStore() *Store {
	return &Store{
		StoreIndex: &StoreIndex{
			Index:      map[string]IndexRecord{},
			SortedKeys: []string{},
		},
		readerPool: NewEmptyReaderPool(),
	}
}

func NewStoreFromRecords(openReaderSeekCloser OpenReaderSeekCloser) (store *Store, err error) {
	return NewStoreFromRecordsWithConfig(openReaderSeekCloser, Config{
		MaxConnections: 100,
		DefaultTimeout: 100 * time.Millisecond,
		DrainTimeout:   1 * time.Second,
	})
}

type ErrReadingIndex struct {
	Err error
}

func (e *ErrReadingIndex) Error() string {
	return fmt.Sprintf("error reading index %s", e.Err)
}

type ErrCreatingPool struct {
	Err error
}

func (e *ErrCreatingPool) Error() string {
	return fmt.Sprintf("error reading index %s", e.Err)
}

func NewStoreFromRecordsWithIndexAndConfig(openReaderSeekCloser OpenReaderSeekCloser, index io.Reader, config Config) (store *Store, err error) {
	store = &Store{}

	storeIndex, err := ReadJsonlIndex(index, config.KeysDontNeedSorting)
	if err != nil {
		return nil, &ErrReadingIndex{err}
	}
	store.StoreIndex = storeIndex
	store.readerPool, err = NewReaderPoolAdvanced(openReaderSeekCloser, config.MaxConnections, config.DefaultTimeout, config.DrainTimeout)
	if err != nil {
		return nil, &ErrCreatingPool{err}
	}
	return store, nil
}

func NewStoreFromRecordsWithIndex(openReaderSeekCloser OpenReaderSeekCloser, index io.Reader) (store *Store, err error) {
	return NewStoreFromRecordsWithIndexAndConfig(openReaderSeekCloser, index, Config{
		MaxConnections:      100,
		DefaultTimeout:      100 * time.Millisecond,
		DrainTimeout:        1 * time.Second,
		KeysDontNeedSorting: true,
	})
}
