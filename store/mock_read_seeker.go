package store

import "io"

type MockReaderSeekCloser struct {
	readSeeker io.ReadSeeker
}

func (m *MockReaderSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return m.readSeeker.Seek(offset, whence)
}

func (m *MockReaderSeekCloser) Read(p []byte) (n int, err error) {
	return m.readSeeker.Read(p)
}

func (*MockReaderSeekCloser) Close() error {
	return nil
}

func NewReadSeekCloser(readSeeker io.ReadSeeker) io.ReadSeekCloser {
	return &MockReaderSeekCloser{readSeeker: readSeeker}
}
