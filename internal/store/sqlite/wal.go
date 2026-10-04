package sqlite

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

// The wal-index (the database's "-shm" file) records how far the write-ahead log has
// grown and how much of it a checkpoint has copied back into the database. Its layout
// is part of SQLite's documented file format (https://www.sqlite.org/walformat.html,
// "The WAL-Index Format"), in the host's byte order:
//
//	offset  0  WalIndexHdr, first copy (48 bytes)
//	       14    u16 szPage (1 means 65536)
//	       16    u32 mxFrame: the last valid frame of the log
//	offset 48  WalIndexHdr, second copy
//	offset 96  WalCkptInfo
//	       96    u32 nBackfill: the frames already copied into the database
//
// Bytes from 120 on hold the locks; on Windows a read overlapping a locked byte
// fails, so only the first walIndexPrefix bytes are read.
const (
	walIndexPrefix = 100
	offPageSize    = 14
	offMaxFrame    = 16
	offBackfill    = 96
)

// PendingLog returns how many bytes of the write-ahead log of the database at path
// no checkpoint has copied back yet: the frames past nBackfill, times the page size.
// It is 0 when the database has no wal-index.
func PendingLog(path string) (int64, error) {
	f, err := os.Open(path + "-shm")
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var b [walIndexPrefix]byte
	if _, err := io.ReadFull(f, b[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil
		}
		return 0, err
	}
	order := binary.NativeEndian
	page := int64(order.Uint16(b[offPageSize:]))
	if page == 1 {
		page = 65536
	}
	mx, backfilled := int64(order.Uint32(b[offMaxFrame:])), int64(order.Uint32(b[offBackfill:]))
	return max(0, mx-backfilled) * page, nil
}
