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
	// the named point: "chunk" after each chunk, "commit" after switchTo's
	// transaction genuinely commits (simulating the race where the caller
	// never learns the commit landed), or "cleanup" after the pointer
	// switch commits and Put has acted on that.
	crash func(point string) bool

	// failAfterCommit, when set (tests only), is called right after Put's
	// pointer-switch transaction commits; a non-nil result is returned from
	// Put instead of nil, exercising the case where something goes wrong
	// between commit and return (a cancelled context, a transient error)
	// without touching the chunks that are now live.
	failAfterCommit func() error
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

// Put implements [BlobStore]. Until switchTo's transaction makes upload the live one,
// a failure leaves upload this call's own, abandoned attempt, which Put removes. From
// the switch on, upload's chunks are never removed here, however Put goes on to fail (a
// cancelled context, a transient error, a test's injected failure). An ambiguous
// switch counts as made: upload may be live, and the upload it replaced may or may not
// be garbage (the commit may have rolled back, leaving it current), so both are left
// to a later Sweep, or to upload's own eventual removal. The caller cannot be told
// what happened; it must Stat or Get to find out.
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
	q := &s.d.Blobs
	if _, err := s.w.ExecContext(ctx, q.Register, upload, name); err != nil {
		return info, fmt.Errorf("register blob upload: %w", err)
	}
	committed := false
	defer func() {
		if err != nil && !committed && !errors.Is(err, errSimulatedCrash) {
			b.removeUpload(context.WithoutCancel(ctx), upload)
		}
	}()

	h := sha256.New()
	buf := make([]byte, s.chunk)
	var size int64
	chunks := 0
	for {
		n, rerr := io.ReadFull(r, buf)
		if n > 0 {
			h.Write(buf[:n])
			if _, err := s.w.ExecContext(ctx, q.WriteChunk, upload, chunks, buf[:n]); err != nil {
				return info, fmt.Errorf("write blob chunk %d: %w", chunks, err)
			}
			chunks++
			size += int64(n)
			if b.crashAt("chunk") {
				return info, errSimulatedCrash
			}
			if chunks%blobTouchEvery == 0 {
				res, err := s.w.ExecContext(ctx, q.Touch, upload)
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

	old, createdAt, err := b.switchTo(ctx, name, upload, size, chunks, sum)
	if errors.Is(err, ErrAmbiguousCommit) {
		committed = true
		return info, err
	}
	if err != nil {
		return info, err
	}
	committed = true
	info = BlobInfo{Name: name, Size: size, Chunks: chunks, SHA256: sum, CreatedAt: millis(createdAt)}
	if b.crashAt("cleanup") {
		return info, errSimulatedCrash
	}
	if old != "" {
		b.removeUpload(context.WithoutCancel(ctx), old)
	}
	if b.failAfterCommit != nil {
		if ferr := b.failAfterCommit(); ferr != nil {
			return info, ferr
		}
	}
	return info, nil
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

// switchTo points name at upload and returns the upload it replaced (now
// registered for removal) and the commit timestamp it stamped on the row,
// so the caller can report the result without a second read.
//
// One read of the database clock serves both the row and the return value, so they
// can never disagree. The upload must still be registered, or Sweep has started
// removing it. The blob's row is created if need be and locked, so that of two
// concurrent Puts the second sees the first's upload as the one it replaces. A failed
// commit is ErrAmbiguousCommit: ctx may have been cancelled, or the connection
// dropped, right as the server decided, so the caller must not act as if it knows the
// outcome either way. The "commit" crash point reports a commit that landed as
// ambiguous, to simulate that race.
func (b *blobStore) switchTo(ctx context.Context, name, upload string, size int64, chunks int, sum string) (old string, createdAt int64, err error) {
	s := b.s
	q := &s.d.Blobs
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return "", 0, err
	}
	defer rollback(tx)
	if err := tx.QueryRowContext(ctx, q.Clock).Scan(&createdAt); err != nil {
		return "", 0, err
	}
	res, err := tx.ExecContext(ctx, q.Unregister, upload)
	if err := sweptCheck(res, err, name); err != nil {
		return "", 0, err
	}
	row, err := returningRow(ctx, tx, q.Take, []any{name}, []any{name})
	if err != nil {
		return "", 0, err
	}
	if err := row.Scan(&old); err != nil {
		return "", 0, err
	}
	if _, err := tx.ExecContext(ctx, q.Point, upload, size, chunks, sum, createdAt, name); err != nil {
		return "", 0, err
	}
	if old != "" {
		if err := b.registerGarbage(ctx, tx, old, name); err != nil {
			return "", 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", 0, fmt.Errorf("switch blob %q to upload %s: %w: %w", name, upload, ErrAmbiguousCommit, err)
	}
	if b.crashAt("commit") {
		return "", 0, fmt.Errorf("switch blob %q to upload %s: %w: simulated", name, upload, ErrAmbiguousCommit)
	}
	return old, createdAt, nil
}

// registerGarbage records an upload that is no longer referenced with
// touched_at 0, so Sweep removes it if nothing else does.
func (b *blobStore) registerGarbage(ctx context.Context, tx *sql.Tx, upload, name string) error {
	_, err := tx.ExecContext(ctx, b.s.d.Blobs.RegisterGarbage, upload, name)
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
	if _, err := b.s.w.ExecContext(ctx, b.s.d.Blobs.Unregister, upload); err != nil {
		b.s.log.WarnContext(ctx, "could not remove a blob upload", slog.String("upload_id", upload), slog.Any("error", err))
	}
}

// deleteChunks deletes an upload's chunks in short statements.
func (b *blobStore) deleteChunks(ctx context.Context, upload string) error {
	s := b.s
	var maxChunk sql.NullInt64
	if err := s.r.QueryRowContext(ctx, s.d.Blobs.MaxChunk, upload).Scan(&maxChunk); err != nil {
		return err
	}
	q := s.d.Blobs.DeleteChunks
	for lo := int64(0); maxChunk.Valid && lo <= maxChunk.Int64; lo += blobDeleteStep {
		if _, err := s.w.ExecContext(ctx, q, upload, lo, lo+blobDeleteStep); err != nil {
			return err
		}
	}
	return nil
}

// Sweep implements [BlobStore]. It removes registered uploads that are unreferenced
// and stale, deleting each registration first, and only while it is still stale (one
// touched meanwhile is still being written), so a Put still writing it fails rather
// than commit a blob with missing chunks. Then it removes the chunks of uploads that
// are neither referenced nor registered: left by an interrupted cleanup, or written
// after a sweep took the upload. Uploads register before their first chunk, so those
// are never live.
func (b *blobStore) Sweep(ctx context.Context, olderThan time.Duration) (removed int, err error) {
	s := b.s
	ctx, end := s.start(ctx, "blob_sweep")
	defer end(&err)
	if olderThan < 0 {
		return 0, invalidf("sweep age %s", olderThan)
	}
	ms := olderThan.Milliseconds()

	stale, err := b.uploadIDs(ctx, s.d.Blobs.Stale, ms)
	if err != nil {
		return 0, err
	}
	for _, id := range stale {
		res, err := s.w.ExecContext(ctx, s.d.Blobs.ClaimStale, id, ms)
		if err != nil {
			return removed, err
		}
		if n, err := res.RowsAffected(); err != nil {
			return removed, err
		} else if n == 0 {
			continue
		}
		if err := b.deleteChunks(ctx, id); err != nil {
			return removed, err
		}
		removed++
	}

	orphans, err := b.uploadIDs(ctx, s.d.Blobs.Orphans)
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
	rows, err := b.s.r.QueryContext(ctx, q, args...)
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
	err := s.r.QueryRowContext(ctx, s.d.Blobs.Stat, name).
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
// is none. It stays valid UTF-8, which Postgres text requires: the last rune is
// incremented, skipping the surrogates, and a last rune with no successor is dropped
// for the one before it to carry.
func prefixEnd(prefix string) (string, bool) {
	for prefix != "" {
		r, size := utf8.DecodeLastRuneInString(prefix)
		prefix = prefix[:len(prefix)-size]
		switch r {
		case utf8.MaxRune:
			continue
		case 0xD7FF:
			r = 0xE000
		default:
			r++
		}
		return prefix + string(r), true
	}
	return "", false
}

// List implements [BlobStore]. Names compare byte for byte on every dialect, so the
// prefix is a key range.
func (b *blobStore) List(ctx context.Context, prefix string) (out []BlobInfo, err error) {
	s := b.s
	ctx, end := s.start(ctx, "blob_list", attribute.String("prefix", prefix))
	defer end(&err)
	q, args := s.d.Blobs.ListFrom, []any{prefix}
	if hi, ok := prefixEnd(prefix); ok {
		q, args = s.d.Blobs.ListRange, []any{prefix, hi}
	}
	rows, err := s.r.QueryContext(ctx, q, args...)
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
			break
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
	err = tx.QueryRowContext(ctx, s.d.Blobs.LockForDelete, name).Scan(&upload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, s.d.Blobs.Delete, name); err != nil {
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
		err := r.s.r.QueryRowContext(r.ctx, r.s.d.Blobs.ReadChunk,
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
