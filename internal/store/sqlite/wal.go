package sqlite

import (
	"bytes"
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
//	        0    u32 iVersion: 3007000, the format this reader knows
//	       14    u16 szPage (1 means 65536)
//	       16    u32 mxFrame: the last valid frame of the log
//	offset 48  WalIndexHdr, second copy
//	offset 96  WalCkptInfo
//	       96    u32 nBackfill: the frames already copied into the database
//
// Bytes from 120 on hold the locks; on Windows a read overlapping a locked byte
// fails, so only the first walIndexPrefix bytes are read. The header is read without
// SQLite's locks: a read that catches a writer mid-update finds the two copies
// differ, and is not trusted.
const (
	walIndexPrefix  = 100
	walIndexHdrSize = 48
	walIndexVersion = 3007000
	offPageSize     = 14
	offMaxFrame     = 16
	offBackfill     = 96
)

// PendingLog returns how many bytes of the write-ahead log of the database at path
// no checkpoint has copied back yet: the frames past nBackfill, times the page size.
// When the wal-index cannot tell (there is none, as with locking_mode EXCLUSIVE; it is
// of another version; or it was caught mid-update), it returns the log file's size,
// which overstates what is pending and so never skips a checkpoint that is due.
func PendingLog(path string) (int64, error) {
	b, ok, err := readWALIndex(path + "-shm")
	if err != nil {
		return 0, err
	}
	if !ok {
		return logFileSize(path + "-wal")
	}
	order := binary.NativeEndian
	page := int64(order.Uint16(b[offPageSize:]))
	if page == 1 {
		page = 65536
	}
	mx, backfilled := int64(order.Uint32(b[offMaxFrame:])), int64(order.Uint32(b[offBackfill:]))
	return max(0, mx-backfilled) * page, nil
}

func readWALIndex(path string) (b [walIndexPrefix]byte, ok bool, err error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return b, false, nil
	}
	if err != nil {
		return b, false, err
	}
	defer f.Close()
	if _, err := io.ReadFull(f, b[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return b, false, nil
		}
		return b, false, err
	}
	consistent := bytes.Equal(b[:walIndexHdrSize], b[walIndexHdrSize:2*walIndexHdrSize])
	return b, consistent && binary.NativeEndian.Uint32(b[:4]) == walIndexVersion, nil
}

func logFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
