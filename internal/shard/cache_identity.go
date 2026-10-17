package shard

import (
	"encoding/json"
	"sync/atomic"
)

// copyIDs numbers the shard copies the process opens.
var copyIDs atomic.Uint64

// CacheIdentity names what a generation's searches see, for caches of their results
// (search's request cache). It has two keys:
//
//   - The copy's own: Copy, Gen and MappingVersion. Two generations of one open copy
//     with the same three expose the same documents under the same mapping, so a
//     result is reusable between them however far Seq moved; any refresh that writes,
//     delete, merge or remap changes them.
//   - The copy-independent one: IndexUID, Shard, MappingVersion, Seq and SeqMark. Every
//     copy of a shard that has applied the changelog up to Seq holds the same
//     documents, whatever its segments. SeqMark, the mark of the change at Seq, tells
//     the same Seq of two changelog histories apart (a database failover that lost
//     commits reissues their seqs to other changes). It holds only when SeqMark is set
//     and MappingVersion and Seq are above 0: see [CacheIdentity.Shareable].
type CacheIdentity struct {
	// Copy is the open copy's number, unique in the process (a reopened copy gets a
	// new one).
	Copy uint64
	// Gen is [Generation.Gen].
	Gen uint64
	// Index and Shard name the shard.
	Index string
	Shard int
	// IndexUID is the index incarnation ("" when none was seen).
	IndexUID string
	// MappingVersion is [Generation.MappingVersion].
	MappingVersion int64
	// Seq is [Generation.Seq].
	Seq int64
	// SeqMark is the changelog mark of the change at Seq, as the copy recorded it; nil
	// when it recorded none.
	SeqMark json.RawMessage
}

// Shareable reports whether the copy-independent key holds: the index incarnation,
// the mapping's version and the mark of the change at Seq are all known.
func (c CacheIdentity) Shareable() bool {
	return c.IndexUID != "" && c.MappingVersion > 0 && c.Seq > 0 && len(c.SeqMark) > 0
}

// CacheIdentity returns the generation's [CacheIdentity].
func (g *Generation) CacheIdentity() CacheIdentity {
	return CacheIdentity{
		Copy:           g.shard.copyID,
		Gen:            g.gen,
		Index:          g.shard.opts.Index,
		Shard:          g.shard.opts.Shard,
		IndexUID:       g.uid,
		MappingVersion: g.mp.version,
		Seq:            g.seq,
		SeqMark:        g.marks.seq,
	}
}
