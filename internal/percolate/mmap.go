package percolate

import (
	"errors"
	"os"
	"sync"
)

// mapping is a query segment file mapped read-only. Its bytes are valid until close.
type mapping struct {
	f      *os.File
	data   []byte
	unmap  func() error
	closed sync.Once
	err    error
}

// mapFile maps the file at path read-only.
func mapFile(path string) (*mapping, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if info.Size() == 0 || int64(int(info.Size())) != info.Size() {
		_ = f.Close()
		return nil, &CorruptError{Path: path, Why: "bad file size"}
	}
	data, unmap, err := platformMap(f, int(info.Size()))
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &mapping{f: f, data: data, unmap: unmap}, nil
}

// close unmaps the file and closes it, once.
func (m *mapping) close() error {
	m.closed.Do(func() {
		m.data = nil
		m.err = errors.Join(m.unmap(), m.f.Close())
	})
	return m.err
}
