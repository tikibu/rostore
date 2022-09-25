package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"sort"
)

type CurrentFile struct {
	FileName      string `json:"filename"`
	IndexFileName string `json:"index_filename"`
}

type IndexRecord struct {
	Key    string `json:"key"`
	Offset int64  `json:"offset"`
	Len    int    `json:"len"`
	Type   string `json:"type"`
}

type StoreIndex struct {
	SortedKeys []string
	Index      map[string]IndexRecord
}

var ErrIndexKeySerialization = errors.New("during index serialization key not found in index")

func (s *StoreIndex) WriteJsonl(out io.Writer) (err error) {
	encoder := json.NewEncoder(out)
	for _, key := range s.SortedKeys {
		indexRecord, ok := s.Index[key]
		if !ok {
			return ErrIndexKeySerialization
		}
		encoder.Encode(indexRecord)
	}
	return nil
}

func ReadJsonlIndex(in io.Reader, keysDontNeedSorting bool) (store *StoreIndex, err error) {
	scanner := bufio.NewScanner(in)
	store = &StoreIndex{}
	store.Index = make(map[string]IndexRecord)
	for scanner.Scan() {
		var indexRecord IndexRecord
		err = json.Unmarshal(scanner.Bytes(), &indexRecord)
		if err != nil {
			return store, err
		}
		store.Index[indexRecord.Key] = indexRecord
		store.SortedKeys = append(store.SortedKeys, indexRecord.Key)
	}

	if !keysDontNeedSorting {
		sort.Strings(store.SortedKeys)
	}

	return store, err
}
