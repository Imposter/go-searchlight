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
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
)

// blobStore implements BlobStore.
//
// A blob's bytes live in sl_blob_chunks under an upload id, and its sl_blobs
// row points at the current upload. Put first registers its upload in
// sl_blob_uploads, then writes the chunks one short statement at a time,
// touching the registration as it goes. A small transaction then switches
// the pointer, drops the new upload's registration and registers the
// replaced upload as garbage (touched_at 0), so the old upload's cleanup is
// durable even if the process dies before doing it. No large bundle ever
// holds a lock for long (SQLite's write lock above all), and readers see the
// old blob or the new one.
//
// Sweep removes what crashes leave behind: registered uploads that are
// unreferenced and untouched for a while, and chunks of uploads that are
// neither referenced nor registered.
type blobStore struct {
	s *sqlStore

	// crash, when set (tests only), makes Put stop dead, with no cleanup, at
	// the named point: "chunk" after each chunk, or "cleanup" after the
	// pointer switch commits.
	crash func(point string) bool
}

// Blob store tuning.
const (
	// blobCleanupTimeout bounds the removal of one failed or replaced upload.
	blobCleanupTimeout = time.Minute
	// blobTouchEvery is how many chunks Put writes between touches of its
	// registration; Sweep's olderThan must be far longer than that takes.
	blobTouchEvery = 16
	// blobDeleteStep is how many chunks one cleanup statement deletes.
	blobDeleteStep = 16
)

// errSimulatedCrash is what Put returns when a test's crash hook fires.
var errSimulatedCrash = errors.New("store: simulated crash")

func validBlobName(name string) error { return validKey("blob name", name, MaxBlobName) }

func newUploadID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (b *blobStore) crashAt(point string) bool { return b.crash != nil && b.crash(point) }

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
	reg := s.bind("INSERT INTO sl_blob_uploads (upload_id, name, touched_at) VALUES (?, ?, " + s.d.Now + ")")
	if _, err := s.w.ExecContext(ctx, reg, upload, name); err != nil {
		return info, fmt.Errorf("register blob upload: %w", err)
	}
	defer func() {
		if err != nil && !errors.Is(err, errSimulatedCrash) {
			b.removeUpload(context.WithoutCancel(ctx), upload)
		}
	}()

	ins := s.bind("INSERT INTO sl_blob_chunks (upload_id, chunk, data) VALUES (?, ?, ?)")
	touch := s.bind("UPDATE sl_blob_uploads SET touched_at = " + s.d.Now + " WHERE upload_id = ?")
	h := sha256.New()
	buf := make([]byte, s.chunk)
	var size int64
	chunks := 0
	for {
		n, rerr := io.ReadFull(r, buf)
		if n > 0 {
			h.Write(buf[:n])
			if _, err := s.w.ExecContext(ctx, ins, upload, chunks, buf[:n]); err != nil {
				return info, fmt.Errorf("write blob chunk %d: %w", chunks, err)
			}
			chunks++
			size += int64(n)
			if b.crashAt("chunk") {
				return info, errSimulatedCrash
			}
			if chunks%blobTouchEvery == 0 {
				res, err := s.w.ExecContext(ctx, touch, upload)
				if err := sweptCheck(res, err, name); err != nil {
					return info, err
				}
			}
		}
		if errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
		if rerr != nil {
			return info, fmt.Errorf("read blob: %w", rerr)
		}
	}
	sum := hex.EncodeToString(h.Sum(nil))

	old, err := b.switchTo(ctx, name, upload, size, chunks, sum)
	if err != nil {
		return info, err
	}
	if b.crashAt("cleanup") {
		return info, errSimulatedCrash
	}
	if old != "" {
		b.removeUpload(context.WithoutCancel(ctx), old)
	}
	return b.Stat(ctx, name)
}

// sweptCheck turns an update of an upload's registration that matched no
// row into an error: Sweep judged the upload abandoned and removed it.
func sweptCheck(res sql.Result, err error, name string) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("blob %q: the upload was swept as abandoned: %w", name, ErrConflict)
	}
	return nil
}

// switchTo points name at upload and returns the upload it replaced, now
// registered for removal.
func (b *blobStore) switchTo(ctx context.Context, name, upload string, size int64, chunks int, sum string) (string, error) {
	s := b.s
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return "", err
	}
	defer rollback(tx)
	// The upload must still be registered, or Sweep has started removing it.
	res, err := tx.ExecContext(ctx, s.bind("DELETE FROM sl_blob_uploads WHERE upload_id = ?"), upload)
	if err := sweptCheck(res, err, name); err != nil {
		return "", err
	}
	// Make sure the row exists and lock it, so that of two concurrent Puts
	// the second sees the first's upload as the one it replaces.
	ensure := "INSERT INTO sl_blobs (name, upload_id, size, chunks, sha256, created_at) VALUES (?, '', 0, 0, '', 0)" +
		s.d.Upsert([]string{"name"}, []string{"name"})
	if _, err := tx.ExecContext(ctx, s.bind(ensure), name); err != nil {
		return "", err
	}
	var old string
	if err := tx.QueryRowContext(ctx, s.bind("SELECT upload_id FROM sl_blobs WHERE name = ?"+s.d.ForUpdate), name).Scan(&old); err != nil {
		return "", err
	}
	set := "UPDATE sl_blobs SET upload_id = ?, size = ?, chunks = ?, sha256 = ?, created_at = " + s.d.Now + " WHERE name = ?"
	if _, err := tx.ExecContext(ctx, s.bind(set), upload, size, chunks, sum, name); err != nil {
		return "", err
	}
	if old != "" {
		if err := b.registerGarbage(ctx, tx, old, name); err != nil {
			return "", err
		}
	}
	return old, tx.Commit()
}

// registerGarbage records an upload that is no longer referenced with
// touched_at 0, so Sweep removes it if nothing else does.
func (b *blobStore) registerGarbage(ctx context.Context, tx *sql.Tx, upload, name string) error {
	_, err := tx.ExecContext(ctx, b.s.bind("INSERT INTO sl_blob_uploads (upload_id, name, touched_at) VALUES (?, ?, 0)"), upload, name)
	return err
}

// removeUpload deletes an upload's chunks a few at a time and then its
// registration. Failures are logged, not returned: the blob itself is
// already consistent, and Sweep finishes the job.
func (b *blobStore) removeUpload(ctx context.Context, upload string) {
	ctx, cancel := context.WithTimeout(ctx, blobCleanupTimeout)
	defer cancel()
	if err := b.deleteChunks(ctx, upload); err != nil {
		b.s.log.WarnContext(ctx, "could not remove a blob upload", slog.String("upload_id", upload), slog.Any("error", err))
		return
	}
	if _, err := b.s.w.ExecContext(ctx, b.s.bind("DELETE FROM sl_blob_uploads WHERE upload_id = ?"), upload); err != nil {
		b.s.log.WarnContext(ctx, "could not remove a blob upload", slog.String("upload_id", upload), slog.Any("error", err))
	}
}

// deleteChunks deletes an upload's chunks in short statements.
func (b *blobStore) deleteChunks(ctx context.Context, upload string) error {
	s := b.s
	var maxChunk sql.NullInt64
	if err := s.r.QueryRowContext(ctx, s.bind("SELECT MAX(chunk) FROM sl_blob_chunks WHERE upload_id = ?"), upload).Scan(&maxChunk); err != nil {
		return err
	}
	q := s.bind("DELETE FROM sl_blob_chunks WHERE upload_id = ? AND chunk >= ? AND chunk < ?")
	for lo := int64(0); maxChunk.Valid && lo <= maxChunk.Int64; lo += blobDeleteStep {
		if _, err := s.w.ExecContext(ctx, q, upload, lo, lo+blobDeleteStep); err != nil {
			return err
		}
	}
	return nil
}

func (b *blobStore) Sweep(ctx context.Context, olderThan time.Duration) (removed int, err error) {
	s := b.s
	ctx, end := s.start(ctx, "blob_sweep")
	defer end(&err)
	if olderThan < 0 {
		return 0, invalidf("sweep age %s", olderThan)
	}
	ms := olderThan.Milliseconds()

	// Registered uploads that are unreferenced and stale. Deleting the
	// registration first (only if still stale) makes a Put still writing it
	// fail rather than commit a blob with missing chunks.
	stale, err := b.uploadIDs(ctx, "SELECT upload_id FROM sl_blob_uploads u WHERE touched_at < "+s.d.Now+" - ?"+
		" AND NOT EXISTS (SELECT 1 FROM sl_blobs b WHERE b.upload_id = u.upload_id)", ms)
	if err != nil {
		return 0, err
	}
	claim := s.bind("DELETE FROM sl_blob_uploads WHERE upload_id = ? AND touched_at < " + s.d.Now + " - ?")
	for _, id := range stale {
		res, err := s.w.ExecContext(ctx, claim, id, ms)
		if err != nil {
			return removed, err
		}
		if n, err := res.RowsAffected(); err != nil {
			return removed, err
		} else if n == 0 {
			continue // touched meanwhile: still being written
		}
		if err := b.deleteChunks(ctx, id); err != nil {
			return removed, err
		}
		removed++
	}

	// Chunks of uploads that are neither referenced nor registered: left by
	// an interrupted cleanup, or written after a sweep took the upload.
	// Uploads register before their first chunk, so these are never live.
	orphans, err := b.uploadIDs(ctx, "SELECT DISTINCT upload_id FROM sl_blob_chunks c"+
		" WHERE NOT EXISTS (SELECT 1 FROM sl_blobs b WHERE b.upload_id = c.upload_id)"+
		" AND NOT EXISTS (SELECT 1 FROM sl_blob_uploads u WHERE u.upload_id = c.upload_id)")
	if err != nil {
		return removed, err
	}
	for _, id := range orphans {
		if err := b.deleteChunks(ctx, id); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

func (b *blobStore) uploadIDs(ctx context.Context, q string, args ...any) ([]string, error) {
	rows, err := b.s.r.QueryContext(ctx, b.s.bind(q), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
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

// Get's span covers the whole stream: it ends when the reader is closed.
func (b *blobStore) Get(ctx context.Context, name string) (rc io.ReadCloser, info BlobInfo, err error) {
	ctx, end := b.s.start(ctx, "blob_get", attribute.String("blob", name))
	info, upload, err := b.stat(ctx, name)
	if err != nil {
		end(&err)
		return nil, info, err
	}
	return &blobReader{ctx: ctx, s: b.s, info: info, upload: upload, h: sha256.New(), end: end}, info, nil
}

// prefixEnd returns the smallest string greater than every string that
// starts with prefix, in byte (and so code point) order, or false when there
// is none. It stays valid UTF-8, which Postgres text requires.
func prefixEnd(prefix string) (string, bool) {
	for prefix != "" {
		r, size := utf8.DecodeLastRuneInString(prefix)
		prefix = prefix[:len(prefix)-size]
		switch r {
		case utf8.MaxRune:
			continue // no successor; carry into the rune before
		case 0xD7FF:
			r = 0xE000 // skip the surrogates
		default:
			r++
		}
		return prefix + string(r), true
	}
	return "", false
}

func (b *blobStore) List(ctx context.Context, prefix string) (out []BlobInfo, err error) {
	s := b.s
	ctx, end := s.start(ctx, "blob_list", attribute.String("prefix", prefix))
	defer end(&err)
	// Names compare byte for byte on every dialect, so the prefix is a key
	// range.
	q := "SELECT name, size, chunks, sha256, created_at FROM sl_blobs WHERE name >= ?"
	args := []any{prefix}
	if hi, ok := prefixEnd(prefix); ok {
		q += " AND name < ?"
		args = append(args, hi)
	}
	rows, err := s.r.QueryContext(ctx, s.bind(q+" ORDER BY name"), args...)
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
		if !strings.HasPrefix(info.Name, prefix) {
			break // past the range
		}
		info.CreatedAt = millis(created)
		out = append(out, info)
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
	if err := b.registerGarbage(ctx, tx, upload, name); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	b.removeUpload(context.WithoutCancel(ctx), upload)
	return nil
}

// blobReader streams a blob one chunk per query and checks its digest at
// the end. Closing it ends Get's span.
type blobReader struct {
	ctx    context.Context // the Get call's context, which bounds the stream
	s      *sqlStore
	info   BlobInfo
	upload string
	next   int
	buf    []byte
	h      hash.Hash
	err    error
	end    func(*error)
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
	if r.closed {
		return nil
	}
	r.closed = true
	r.buf = nil
	err := r.err
	if errors.Is(err, io.EOF) {
		err = nil
	}
	r.end(&err)
	return nil
}
