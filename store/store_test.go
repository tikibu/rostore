package store

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	//let's mock some records
	records := MockRecords()

	// let's build a mock data jsonl file
	jsonBytes := MockJsonlBytes(records)

	//let's build an index
	index, err := BuildIndex(bytes.NewReader(jsonBytes))
	assert.NoError(t, err)

	//let's write an index
	buf := bytes.Buffer{}
	err = index.WriteJsonl(&buf)
	assert.NoError(t, err)

	//let's load an index
	index2, err := ReadJsonlIndex(bytes.NewReader(buf.Bytes()), false)
	assert.NoError(t, err)
	assert.Equal(t, index, index2)
}

func TestReadingStore(t *testing.T) {
	//let's mock some records
	records := MockRecords()

	// let's build a mock data jsonl file
	recordsBytes := MockJsonlBytes(records)

	//let's build an index
	index, err := BuildIndex(bytes.NewReader(recordsBytes))
	assert.NoError(t, err)

	//let's write an index
	indexBuf := bytes.Buffer{}
	err = index.WriteJsonl(&indexBuf)
	assert.NoError(t, err)

	//let's build a store
	store, err := NewStoreFromRecordsWithIndex(func() (io.ReadSeekCloser, error) {
		return NewReadSeekCloser(bytes.NewReader(recordsBytes)), nil
	},
		bytes.NewReader(indexBuf.Bytes()))

	assert.NotNil(t, store)
	assert.NoError(t, err)

	//let's access each record
	for _, record := range records {
		// let's access the record
		rec, err := store.GetRecord(record.Key)
		assert.NoError(t, err)
		assert.JSONEq(t, record.String(), rec.String())
	}
}

func TestReadingIndex(t *testing.T) {
	//let's mock some records
	records := MockRecords()

	// let's build a mock data jsonl file
	recordsBytes := MockJsonlBytes(records)

	//let's build an index
	index, err := BuildIndex(bytes.NewReader(recordsBytes))
	assert.NoError(t, err)

	//let's write an index
	indexBuf := bytes.Buffer{}
	err = index.WriteJsonl(&indexBuf)
	assert.NoError(t, err)

	//let's build a store
	store, err := NewStoreFromRecordsWithIndex(func() (io.ReadSeekCloser, error) {
		return NewReadSeekCloser(bytes.NewReader(recordsBytes)), nil
	},
		bytes.NewReader(indexBuf.Bytes()))

	assert.NotNil(t, store)
	assert.NoError(t, err)

	//let's access each record
	for _, record := range records {
		// let's access the record
		idx, err := store.GetRecordIndex(record.Key)
		assert.NoError(t, err)
		assert.Equal(t, record.Key, idx.Key)
		assert.Equal(t, record.Type, idx.Type)
	}
}
