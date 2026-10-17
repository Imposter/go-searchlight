package replica

import "encoding/json"

// OwnContentMark names what a copy holds, for caches shared by every copy of its shard
// (search's request cache): the mark of the shard's newest change the copy holds, when
// own (the copy's own mark, [shard.Shard.OwnMark], as of a generation of seq) says
// which it is up to seq, and a poll has checked it against the changelog. Every copy
// that holds the shard's changes up to that change holds the same documents, however
// far its seq moved with other shards' and indexes' writes. Nil when it is not known:
// no own mark, a state up to another seq, no change of the shard yet, or a newer
// change of the shard than the checked one, not yet checked (a copy that applied a
// page read across a changelog rewind holds a mix no other copy does, until the next
// poll finds it out), or a change written before nonces (a reissue to the same
// document and kind in the same millisecond would have the same mark).
func OwnContentMark(own json.RawMessage, seq int64) json.RawMessage {
	checked, pending := decodeOwn(own)
	if seq <= 0 || checked.upTo <= 0 || checked.last.Seq <= 0 || checked.last.Nonce == 0 || checked.upTo > seq {
		return nil
	}
	switch {
	case pending.upTo == 0 && checked.upTo == seq:
	case pending.upTo == seq && pending.last.Same(checked.last) && pending.last.Nonce == checked.last.Nonce:
	default:
		return nil
	}
	return encodeMark(checked.last)
}
