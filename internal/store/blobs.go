package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// blobStore implements BlobStore. A blob's bytes live in sl_blob_chunks
// under an upload id; its sl_blobs row points at the upload that is current.
// Put writes a new upload chunk by chunk, each in its own short statement,
// then switches the pointer in one small transaction and deletes the old
// upload, so a large bundle never holds a lock (SQLite's write lock above
// all) for long, and readers see the old blob or the new one.
type blobStore struct{ s *sqlStore }

// blobCleanupTimeout bounds the removal of a failed or replaced upload.
const blobCleanupTimeout = time.Minute

func validBlobName(name string) error { return validKey("blob name", name, MaxBlobName) }

func newUploadID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (b *blobStore) Put(ctx context.Context, name string, r io.Reader) (info BlobInfo, err error) {
	s := b.s
	ctx, end := s.start(ctx, "blob_put", attribute.String("blob", name))
	defer end(&err)
	if err := validBlobName(name); err != nil {
		return info, err
	}
	upload, err := newUploadID()
	if err != nil {
		return info, err
	}
	written := false
	defer func() {
		if err != nil && written {
			b.deleteUpload(context.WithoutCancel(ctx), upload)
		}
	}()

	ins := s.bind("INSERT INTO sl_blob_chunks (upload_id, chunk, data) VALUES (?, ?, ?)")
	h := sha256.New()
	buf := make([]byte, s.chunk)
	var size int64
	chunks := 0
	for {
		n, rerr := io.ReadFull(r, buf)
		if n > 0 {
			h.Write(buf[:n])
			written = true
			if _, err := s.w.ExecContext(ctx, ins, upload, chunks, buf[:n]); err != nil {
				return info, fmt.Errorf("write blob chunk %d: %w", chunks, err)
			}
			chunks++
			size += int64(n)
		}
		if errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
		if rerr != nil {
			return info, fmt.Errorf("read blob: %w", rerr)
		}
	}
	sum := hex.EncodeToString(h.Sum(nil))

	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return info, err
	}
	defer rollback(tx)
	// Make sure the row exists and lock it (the no-op update locks an
	// existing row), so that of two concurrent Puts the second sees the
	// first's upload as the one it replaces and nothing is orphaned.
	ensure := "INSERT INTO sl_blobs (name, upload_id, size, chunks, sha256, created_at) VALUES (?, '', 0, 0, '', 0)" +
		s.d.Upsert([]string{"name"}, []string{"name"})
	if _, err := tx.ExecContext(ctx, s.bind(ensure), name); err != nil {
		return info, err
	}
	var old string
	if err := tx.QueryRowContext(ctx, s.bind("SELECT upload_id FROM sl_blobs WHERE name = ?"+s.d.ForUpdate), name).Scan(&old); err != nil {
		return info, err
	}
	set := "UPDATE sl_blobs SET upload_id = ?, size = ?, chunks = ?, sha256 = ?, created_at = " + s.d.Now + " WHERE name = ?"
	if _, err := tx.ExecContext(ctx, s.bind(set), upload, size, chunks, sum, name); err != nil {
		return info, err
	}
	if err := tx.Commit(); err != nil {
		return info, err
	}
	if old != "" {
		b.deleteUpload(ctx, old)
	}
	return b.Stat(ctx, name)
}

// deleteUpload removes an upload's chunks a few at a time. Failures are
// logged, not returned: the blob itself is already consistent.
func (b *blobStore) deleteUpload(ctx context.Context, upload string) {
	s := b.s
	ctx, cancel := context.WithTimeout(ctx, blobCleanupTimeout)
	defer cancel()
	q := s.bind("DELETE FROM sl_blob_chunks WHERE upload_id = ? AND chunk >= ? AND chunk < ?")
	var maxChunk sql.NullInt64
	if err := s.r.QueryRowContext(ctx, s.bind("SELECT MAX(chunk) FROM sl_blob_chunks WHERE upload_id = ?"), upload).Scan(&maxChunk); err != nil {
		s.log.WarnContext(ctx, "could not remove a blob upload", slog.String("upload_id", upload), slog.Any("error", err))
		return
	}
	const step = 16
	for lo := int64(0); maxChunk.Valid && lo <= maxChunk.Int64; lo += step {
		if _, err := s.w.ExecContext(ctx, q, upload, lo, lo+step); err != nil {
			s.log.WarnContext(ctx, "could not remove a blob upload", slog.String("upload_id", upload), slog.Any("error", err))
			return
		}
	}
}

func (b *blobStore) stat(ctx context.Context, name string) (BlobInfo, string, error) {
	s := b.s
	info := BlobInfo{Name: name}
	var upload string
	var created int64
	err := s.r.QueryRowContext(ctx, s.bind("SELECT upload_id, size, chunks, sha256, created_at FROM sl_blobs WHERE name = ?"), name).
		Scan(&upload, &info.Size, &info.Chunks, &info.SHA256, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return info, "", fmt.Errorf("blob %q: %w", name, ErrNotFound)
	}
	info.CreatedAt = millis(created)
	return info, upload, err
}

func (b *blobStore) Stat(ctx context.Context, name string) (info BlobInfo, err error) {
	ctx, end := b.s.start(ctx, "blob_stat", attribute.String("blob", name))
	defer end(&err)
	info, _, err = b.stat(ctx, name)
	return info, err
}

func (b *blobStore) Get(ctx context.Context, name string) (rc io.ReadCloser, info BlobInfo, err error) {
	ctx, end := b.s.start(ctx, "blob_get", attribute.String("blob", name))
	defer end(&err)
	info, upload, err := b.stat(ctx, name)
	if err != nil {
		return nil, info, err
	}
	return &blobReader{ctx: ctx, s: b.s, info: info, upload: upload, h: sha256.New()}, info, nil
}

func (b *blobStore) List(ctx context.Context, prefix string) (out []BlobInfo, err error) {
	s := b.s
	ctx, end := s.start(ctx, "blob_list", attribute.String("prefix", prefix))
	defer end(&err)
	rows, err := s.r.QueryContext(ctx, "SELECT name, size, chunks, sha256, created_at FROM sl_blobs ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var info BlobInfo
		var created int64
		if err := rows.Scan(&info.Name, &info.Size, &info.Chunks, &info.SHA256, &created); err != nil {
			return nil, err
		}
		// Prefix matching happens here: LIKE escaping and collations differ
		// per dialect, and there are few blobs.
		if strings.HasPrefix(info.Name, prefix) {
			info.CreatedAt = millis(created)
			out = append(out, info)
		}
	}
	return out, rows.Err()
}

func (b *blobStore) Delete(ctx context.Context, name string) (err error) {
	s := b.s
	ctx, end := s.start(ctx, "blob_delete", attribute.String("blob", name))
	defer end(&err)
	if err := validBlobName(name); err != nil {
		return err
	}
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var upload string
	err = tx.QueryRowContext(ctx, s.bind("SELECT upload_id FROM sl_blobs WHERE name = ?"+s.d.ForUpdate), name).Scan(&upload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, s.bind("DELETE FROM sl_blobs WHERE name = ?"), name); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	b.deleteUpload(ctx, upload)
	return nil
}

// blobReader streams a blob one chunk per query and checks its digest at
// the end.
type blobReader struct {
	ctx    context.Context // the Get call's context, which bounds the stream
	s      *sqlStore
	info   BlobInfo
	upload string
	next   int
	buf    []byte
	h      hash.Hash
	err    error
	closed bool
}

func (r *blobReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, ErrClosed
	}
	for len(r.buf) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.next == r.info.Chunks {
			if hex.EncodeToString(r.h.Sum(nil)) != r.info.SHA256 {
				r.err = fmt.Errorf("blob %q: %w", r.info.Name, ErrChecksum)
			} else {
				r.err = io.EOF
			}
			return 0, r.err
		}
		var data []byte
		err := r.s.r.QueryRowContext(r.ctx, r.s.bind("SELECT data FROM sl_blob_chunks WHERE upload_id = ? AND chunk = ?"),
			r.upload, r.next).Scan(&data)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			r.err = fmt.Errorf("blob %q changed while being read: %w", r.info.Name, ErrNotFound)
			return 0, r.err
		case err != nil:
			r.err = err
			return 0, err
		}
		r.h.Write(data)
		r.buf = data
		r.next++
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *blobReader) Close() error {
	r.closed = true
	r.buf = nil
	return nil
}
