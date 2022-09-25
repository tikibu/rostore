package store

import (
	"bufio"
	"encoding/json"
	"io"
	"sort"
)

// This is an extremely simple implementation that assumes that newline
// is only one symbol long
func BuildIndex(in io.Reader) (store *StoreIndex, err error) {
	scanner := bufio.NewScanner(in)
	store = &StoreIndex{}
	store.Index = make(map[string]IndexRecord)
	offset := int64(0)
	for scanner.Scan() {
		var record Record
		bts := scanner.Bytes()
		err = json.Unmarshal(bts, &record)
		if err != nil {
			return store, err
		}
		indexRecord := IndexRecord{
			Key:    record.Key,
			Offset: offset,
			Len:    len(bts),
			Type:   record.Type,
		}
		offset += int64(len(bts)) + 1 // +1 is for newline
		store.Index[record.Key] = indexRecord
		store.SortedKeys = append(store.SortedKeys, record.Key)
	}

	sort.Strings(store.SortedKeys)

	return store, err
}
