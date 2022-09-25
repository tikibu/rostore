package store

import (
	"errors"
	"fmt"
	"io"
	"time"
)

type OpenReaderSeekCloser func() (io.ReadSeekCloser, error)

// ReaderPool is a NON-lazy pool of io.ReaderSeekCloser
// TODO: make it lazy later
type ReaderPool struct {
	readerPool     chan io.ReadSeekCloser
	defaultTimeout time.Duration
	drainTimeout   time.Duration
	drained        bool
}

// Always timeouting EmptyReaderPool
func NewEmptyReaderPool() *ReaderPool {
	return &ReaderPool{
		readerPool:     make(chan io.ReadSeekCloser),
		defaultTimeout: time.Millisecond,
		drainTimeout:   time.Millisecond,
	}
}

func NewReaderPool(openReaderSeekCloser OpenReaderSeekCloser) (readerPool *ReaderPool, err error) {
	return NewReaderPoolAdvanced(openReaderSeekCloser, 100, 100*time.Millisecond, 1*time.Second)
}

func NewReaderPoolAdvanced(openReaderSeekCloser OpenReaderSeekCloser,
	maxConnections int,
	defaultTimeout time.Duration,
	drainTimeout time.Duration) (readerPool *ReaderPool, err error) {
	readerPool = &ReaderPool{
		defaultTimeout: defaultTimeout,
		drainTimeout:   drainTimeout,
		readerPool:     make(chan io.ReadSeekCloser, maxConnections),
	}
	for i := 0; i < maxConnections; i++ {
		reader, err := openReaderSeekCloser()
		if err != nil {
			return nil, fmt.Errorf(("error opening reader %w"), err)
		}
		readerPool.readerPool <- reader
	}
	return readerPool, nil
}

var ErrSecuringReaderTimeout = errors.New("timeout securing reader (timed out getting one from the pool")
var ErrSecuringReaderPoolDrained = errors.New("pool is drained, no readers available")
var ErrTimedOutDrainingPool = errors.New("timed out draining pool")

func (p *ReaderPool) DrainWithTimeout(timeout time.Duration) (err error) {
	p.drained = true // insignificant race condition
	for i := 0; i < cap(p.readerPool); i++ {
		select {
		case reader := <-p.readerPool:
			reader.Close()
		case <-time.After(timeout):
			return ErrTimedOutDrainingPool
		}
	}
	return nil
}

func (p *ReaderPool) Drain() (err error) {
	return p.DrainWithTimeout(p.drainTimeout)
}

func (p *ReaderPool) GetReaderWithTimeout(timeout time.Duration) (reader io.ReadSeekCloser, err error) {
	if p.drained { // little race condition has never caused any problems ;)
		return nil, ErrSecuringReaderPoolDrained
	}
	select {
	case reader = <-p.readerPool:
		return reader, nil
	case <-time.After(timeout):
		return nil, ErrSecuringReaderTimeout
	}
}

func (p *ReaderPool) GetReader() (reader io.ReadSeekCloser, err error) {
	return p.GetReaderWithTimeout(p.defaultTimeout)
}

func (p *ReaderPool) ReturnReader(reader io.ReadSeekCloser) {
	p.readerPool <- reader
}
