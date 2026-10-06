package percolate

import (
	"errors"
	"os"
	"sync"
)

// mapping is a query segment file mapped read-only. Its bytes are valid until close.
type mapping struct {
	data   []byte
	unmap  func() error
	closed sync.Once
	err    error
}

// mapFile maps the file at path read-only. The mapping holds the file: its descriptor
// is closed once the file is mapped.
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
	if cerr := f.Close(); err == nil && cerr != nil {
		err = errors.Join(cerr, unmap())
	}
	if err != nil {
		return nil, err
	}
	return &mapping{data: data, unmap: unmap}, nil
}

// close unmaps the file, once.
func (m *mapping) close() error {
	m.closed.Do(func() {
		m.data = nil
		m.err = m.unmap()
	})
	return m.err
}
