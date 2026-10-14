package replica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Rewind detection.
//
// A database failover that loses commits rewinds the changelog: the new primary's
// counter is behind the old one's, and new writes take the lost seqs again. A copy
// that applied the lost changes would never read the new ones at those seqs. So each
// copy keeps the mark (store.ChangeMark) of a change it applied, its anchor, and every
// poll (store Tip) compares it with the change the changelog now holds at that seq.
//
// A poll reads the head and the anchor's change first, then the page after the copy's
// seq: a rewind between the two hands the copy a page of the new history. So a mark
// learned in a step (the page's last change, or the head the copy advanced to) is only
// pending: the next poll checks the anchor once more, and only then makes the pending
// mark the anchor. A mark becomes the anchor only through such a check; a page read
// across a rewind is caught by the anchor it was read after, and every anchor is
// compared at every poll. A copy with a pending mark and no anchor compares the pending
// mark itself. A head behind the copy's seq is a rewind too, once two polls in a row see
// it: one read that reached a lagging server is not enough to rebuild every copy.
//
// Both marks are persisted with the copy (the manifest's anchor_mark and seq_mark,
// through shard.SetAnchorMark and shard.SetMark), so they survive a restart, travel
// with a peer's snapshot or a bundle, and come with a replacement built aside (whose
// catch-up's check state is carried into the tailer as it is): a copy that was down,
// fetched or rebuilt across the failover checks rather than trusts. A copy loaded from
// the store's snapshot takes the mark of the snapshot's seq from the snapshot itself,
// as pending. A copy with no mark at all (one written by an older binary, or a seq
// whose row was gone when the copy moved to it) takes the mark from its first poll.

// rewindCheck is what one copy knows to tell a rewound changelog: its anchor (a change
// it applied, checked at every poll), the pending mark of its applied seq (learned
// since the anchor was last checked), and the applied seq whose head the last poll saw
// behind it.
type rewindCheck struct {
	anchor, pending store.ChangeMark
	behind          int64
}

// checkSeq is the seq a poll from from reads the change at: the anchor's, or from
// when there is none.
func (rc *rewindCheck) checkSeq(from int64) int64 {
	if rc.anchor.Seq > 0 {
		return rc.anchor.Seq
	}
	return from
}

// errHeadBehind is a poll that read a head behind the copy's seq for the first time:
// not caught up, nor a failure; Run polls again after a short pause, and only a head
// still behind then is a rewind.
var errHeadBehind = errors.New("replica: the changelog's head is behind the copy's seq; polling again")

// markJSON is a ChangeMark as a shard's manifest keeps it (opaque to the shard).
type markJSON struct {
	Seq   int64  `json:"seq"`
	Index string `json:"index"`
	Shard int    `json:"shard"`
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	At    int64  `json:"at"` // Unix ms, by the database clock
	Nonce int64  `json:"nonce,omitempty"`
}

func encodeMark(m store.ChangeMark) json.RawMessage {
	if m.Seq <= 0 {
		return nil
	}
	raw, err := json.Marshal(markJSON{Seq: m.Seq, Index: m.Index, Shard: m.Shard, Kind: string(m.Kind), ID: m.ID, At: m.At.UnixMilli(), Nonce: m.Nonce})
	if err != nil {
		return nil
	}
	return raw
}

func decodeMark(raw json.RawMessage) store.ChangeMark {
	var j markJSON
	if len(raw) == 0 || json.Unmarshal(raw, &j) != nil || j.Seq <= 0 {
		return store.ChangeMark{}
	}
	return store.ChangeMark{Seq: j.Seq, Index: j.Index, Shard: j.Shard, Kind: store.Kind(j.Kind), ID: j.ID, At: time.UnixMilli(j.At).UTC(), Nonce: j.Nonce}
}

// markOf returns the check state sh keeps: its anchor mark, and the mark of its
// applied seq as pending (unless it is the anchor).
func markOf(sh *shard.Shard) rewindCheck {
	rc := rewindCheck{anchor: decodeMark(sh.AnchorMark())}
	if m := decodeMark(sh.SeqMark()); m.Seq == sh.AppliedSeq() && !m.Same(rc.anchor) {
		rc.pending = m
	}
	return rc
}

// recordMark makes m the pending mark of sh's applied seq, in rc and in the shard
// (which flushes it with the seq).
func recordMark(sh *shard.Shard, rc *rewindCheck, m store.ChangeMark) {
	rc.pending = m
	if m.Seq > 0 {
		sh.SetMark(m.Seq, encodeMark(m))
	}
}

// checkRewind compares the changelog, as tip (read at rc.checkSeq(from)) shows it,
// with what the copy rc speaks for applied up to from, its applied seq, and keeps sh's
// persisted anchor in step. Another change at the anchor's seq than the anchor (or,
// with no anchor, at from than the pending mark) means the database lost commits the
// copy applied and has since reissued their seqs: a rewind. A head behind from means
// it too, once the next poll sees it again (wait asks for that poll). A rewound copy is
// rebuilt from the store's snapshot, never from a peer or bundle, which may hold the
// lost commits too. Otherwise the pending mark of from becomes the anchor; with neither,
// the mark is taken from tip. An anchor since pruned, or of a dropped index, cannot be
// compared: it is dropped, and the pending mark is compared with its own seq instead.
func (t *Tailer) checkRewind(ctx context.Context, sh *shard.Shard, rc *rewindCheck, from int64, tip *store.Tip) (wait bool, err error) {
	var cause error
	switch {
	case tip.Head < from && rc.behind != from:
		rc.behind = from
		t.log.WarnContext(ctx, "the changelog's head is behind this copy's seq; polling again to tell a lagging read from a rewind",
			slog.Int64("seq", from), slog.Int64("head", tip.Head))
		return true, nil
	case tip.Head < from:
		cause = fmt.Errorf("the changelog's head %d is behind the copy's seq %d on two polls", tip.Head, from)
	case rc.anchor.Seq > 0 && tip.AtMark.Seq > 0 && !rc.anchor.Same(tip.AtMark):
		cause = differs(rc.anchor, tip.AtMark)
	case rc.anchor.Seq == 0 && rc.pending.Seq == from && tip.AtMark.Seq == from && !rc.pending.Same(tip.AtMark):
		cause = differs(rc.pending, tip.AtMark)
	default:
		rc.behind = 0
		var anchor store.ChangeMark
		switch {
		case rc.anchor.Seq > 0 && tip.AtMark.Seq == 0:
			rc.anchor = store.ChangeMark{}
			sh.SetAnchorMark(nil)
			return false, nil
		case rc.pending.Seq == from:
			anchor = rc.pending
		case rc.anchor.Seq == 0 && tip.AtMark.Seq == from:
			anchor = tip.AtMark
		default:
			return false, nil
		}
		rc.anchor, rc.pending = anchor, store.ChangeMark{}
		sh.SetAnchorMark(encodeMark(anchor))
		return false, nil
	}
	*rc = rewindCheck{}
	t.inst.rewinds.Add(ctx, 1, t.inst.attrs)
	t.log.ErrorContext(ctx, "the changelog no longer holds what this copy applied: the database lost commits; rebuilding the copy from the store",
		slog.Int64("seq", from), slog.Int64("head", tip.Head), slog.Any("error", cause))
	if t.opts.OnRewind != nil {
		t.opts.OnRewind(tip.Head)
	}
	return false, &rebuildError{reason: reasonRewound, err: cause}
}

// differs describes a change the copy applied at a seq that the changelog now holds
// another change at, with each change's commit time and nonce, which may be all that
// tells them apart.
func differs(applied, now store.ChangeMark) error {
	return fmt.Errorf("seq %d is now %s, the copy applied %s there", applied.Seq, describeMark(now), describeMark(applied))
}

// describeMark is a mark as a log line says it.
func describeMark(m store.ChangeMark) string {
	return fmt.Sprintf("%s %q of %s/%d at %d ms, nonce %d", m.Kind, m.ID, m.Index, m.Shard, m.At.UnixMilli(), m.Nonce)
}
