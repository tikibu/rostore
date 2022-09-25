package store

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolLimited(t *testing.T) {
	howManyReaders := 2
	pool, err := NewReaderPoolAdvanced(func() (io.ReadSeekCloser, error) {
		howManyReaders--
		return NewReadSeekCloser(bytes.NewReader([]byte("test"))), nil
	}, howManyReaders, 1*time.Millisecond, 1*time.Millisecond)

	assert.NoError(t, err)

	//first
	r, err := pool.GetReader()
	assert.NoError(t, err)
	assert.NotNil(t, r)

	//second
	r, err = pool.GetReader()
	assert.NoError(t, err)
	assert.NotNil(t, r)

	//but no third
	_, err = pool.GetReader()
	assert.Error(t, err)

	//both of readers were created
	assert.Equal(t, howManyReaders, 0)
}

func TestPoolReturn(t *testing.T) {
	howManyReaders := 2
	pool, err := NewReaderPoolAdvanced(func() (io.ReadSeekCloser, error) {
		howManyReaders--
		return NewReadSeekCloser(bytes.NewReader([]byte("test"))), nil
	}, howManyReaders, 1*time.Millisecond, 1*time.Millisecond)

	assert.NoError(t, err)

	//first
	r, err := pool.GetReader()
	assert.NoError(t, err)
	assert.NotNil(t, r)

	//second
	r, err = pool.GetReader()
	assert.NoError(t, err)
	assert.NotNil(t, r)

	pool.ReturnReader(r)

	//and third
	r2, err := pool.GetReader()
	assert.NoError(t, err)

	//the right reader was returned
	assert.Equal(t, r2, r)
}

func TestDrainedPool(t *testing.T) {
	howManyReaders := 2
	pool, err := NewReaderPoolAdvanced(func() (io.ReadSeekCloser, error) {
		howManyReaders--
		return NewReadSeekCloser(bytes.NewReader([]byte("test"))), nil
	}, howManyReaders, 1*time.Millisecond, 1*time.Millisecond)

	assert.NoError(t, err)

	//first
	r, err := pool.GetReader()
	assert.NoError(t, err)
	assert.NotNil(t, r)

	//second
	r, err = pool.GetReader()
	assert.NoError(t, err)
	assert.NotNil(t, r)

	pool.Drain()

	//and third
	_, err = pool.GetReader()
	assert.Error(t, err, ErrSecuringReaderPoolDrained)
}
