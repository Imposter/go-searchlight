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
//   - The copy-independent one: IndexUID, Shard, MappingVersion and what OwnMark says
//     of the shard's own changes up to Seq (the caller's encoding, which the shard does
//     not read: replica.OwnContentMark reads it). Every copy that holds the shard's
//     changes up to the same last one holds the same documents, whatever its
//     segments, its Seq, or the other shards' writes. The shard's newest applied
//     change ([Generation.MaxSeq]) is no such key: a copy loaded from a snapshot
//     holds no deleted document, so its newest change can be an older one than a
//     tailing copy's with other contents.
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
	// OwnMark is the own mark ([Shard.SetOwnMark]) as of the generation's refresh;
	// nil when none was recorded.
	OwnMark json.RawMessage
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
		OwnMark:        g.marks.own,
	}
}
