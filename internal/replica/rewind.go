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
// A mark may name another shard's change (the head an idle copy advanced to), whose
// row that shard prunes once its own copies are past it: a copy stopped across the
// failover may find nothing to compare at that seq. So each poll also checks the
// copy's own shard, whose rows only a rewind can change while the copy counts for
// pruning: the shard's newest change at or below the seq the copy checked (own.upTo)
// must be the last change of the shard the copy applied there (own.last). A newer one
// is a change the copy skipped, another one at that seq a reissue, and a missing one
// whose seq the shard's prune horizon has not passed was lost. Its state follows the
// same checked/pending split as the marks.
//
// The marks and the own-shard state are persisted with the copy (the manifest's
// anchor_mark, seq_mark and own_mark), so they survive a restart, travel with a peer's
// snapshot or a bundle, and come with a replacement built aside (whose catch-up's
// check state is carried into the tailer as it is): a copy that was down, fetched or
// rebuilt across the failover checks rather than trusts. A copy loaded from the store's
// snapshot takes the mark of the snapshot's seq and its own shard's state from the
// snapshot itself, as pending. A copy with no marks at all (one written by an older
// binary) takes them from its first poll.

// rewindCheck is what one copy knows to tell a rewound changelog: its anchor (a change
// it applied, checked at every poll), the pending mark of its applied seq (learned
// since the anchor was last checked), and the applied seq whose head the last poll saw
// behind it.
type rewindCheck struct {
	anchor, pending store.ChangeMark
	behind          int64
	// own and ownPending are what the copy holds of its own shard: own checked,
	// ownPending as of its applied seq and not yet checked.
	own, ownPending ownCheck
}

// ownCheck is what a copy holds of its own shard up to a seq: last is the shard's
// newest change at or below upTo that the copy applied (the zero mark when there is
// none). upTo 0 means it is not known.
type ownCheck struct {
	last store.ChangeMark
	upTo int64
}

// query is the Tip a poll from from reads: the change at checkSeq(from), and what the
// copy's own shard id holds up to the seq the copy checked it at (from, while it has
// not).
func (rc *rewindCheck) query(id ShardID, from int64) store.TipQuery {
	upTo := rc.own.upTo
	if upTo == 0 {
		upTo = from
	}
	return store.TipQuery{At: rc.checkSeq(from), Shard: id, UpTo: upTo}
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
// applied seq as pending (unless it is the anchor); its own shard's checked state, and
// its pending one when that is as of its applied seq.
func markOf(sh *shard.Shard) rewindCheck {
	rc := rewindCheck{anchor: decodeMark(sh.AnchorMark())}
	if m := decodeMark(sh.SeqMark()); m.Seq == sh.AppliedSeq() && !m.Same(rc.anchor) {
		rc.pending = m
	}
	rc.own, rc.ownPending = decodeOwn(sh.OwnMark())
	if rc.ownPending.upTo != sh.AppliedSeq() {
		rc.ownPending = ownCheck{}
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

// noteApplied records that sh applied a page of its shard's changes ending with last.
func noteApplied(sh *shard.Shard, rc *rewindCheck, last store.ChangeMark) {
	recordMark(sh, rc, last)
	rc.ownPending = ownCheck{last: last, upTo: last.Seq}
	sh.SetOwnMark(encodeOwn(rc.own, rc.ownPending))
}

// noteAdvanced records that sh advanced from from to head, with no change of its shard
// between them; headMark is the change at head. The own-shard state as of from, pending
// or (once a poll promoted it) checked, then holds up to head, as pending.
func noteAdvanced(sh *shard.Shard, rc *rewindCheck, from, head int64, headMark store.ChangeMark) {
	recordMark(sh, rc, headMark)
	switch {
	case from <= 0:
		rc.ownPending = ownCheck{}
	case rc.ownPending.upTo == from:
		rc.ownPending.upTo = head
	case rc.ownPending.upTo == 0 && rc.own.upTo == from:
		rc.ownPending = ownCheck{last: rc.own.last, upTo: head}
	default:
		rc.ownPending = ownCheck{}
	}
	sh.SetOwnMark(encodeOwn(rc.own, rc.ownPending))
}

// ownJSON is a copy's own-shard state as its manifest keeps it (opaque to the shard).
type ownJSON struct {
	Checked *ownStateJSON `json:"checked,omitempty"`
	Pending *ownStateJSON `json:"pending,omitempty"`
}

type ownStateJSON struct {
	Last json.RawMessage `json:"last,omitempty"`
	UpTo int64           `json:"up_to"`
}

func encodeOwn(own, pending ownCheck) json.RawMessage {
	state := func(o ownCheck) *ownStateJSON {
		if o.upTo <= 0 {
			return nil
		}
		return &ownStateJSON{Last: encodeMark(o.last), UpTo: o.upTo}
	}
	j := ownJSON{Checked: state(own), Pending: state(pending)}
	if j.Checked == nil && j.Pending == nil {
		return nil
	}
	raw, err := json.Marshal(j)
	if err != nil {
		return nil
	}
	return raw
}

func decodeOwn(raw json.RawMessage) (own, pending ownCheck) {
	var j ownJSON
	if len(raw) == 0 || json.Unmarshal(raw, &j) != nil {
		return ownCheck{}, ownCheck{}
	}
	state := func(s *ownStateJSON) ownCheck {
		if s == nil || s.UpTo <= 0 {
			return ownCheck{}
		}
		return ownCheck{last: decodeMark(s.Last), upTo: s.UpTo}
	}
	return state(j.Checked), state(j.Pending)
}

// ownRewound compares what the copy checked of its own shard (rc.own) with what tip
// shows of it, the index being the copy's incarnation (uid): a change of the shard at
// or below the seq it checked that the copy never applied, another change than the
// last one it applied at that one's seq, or that change gone though its seq is not
// pruned, is a rewind. Only a rewind can make any of these: the copy applied every
// change of its shard up to its seq, a seq names one change for its life, and pruning
// raises the shard's horizon before it deletes. A dropped or recreated index is not
// compared (the catalogue tells that).
func (rc *rewindCheck) ownRewound(tip *store.Tip, uid string) error {
	o := rc.own
	if o.upTo == 0 || uid == "" || tip.IndexUID != uid {
		return nil
	}
	now := tip.ShardMark
	switch {
	case now.Seq > o.last.Seq:
		return fmt.Errorf("the shard holds %s at seq %d, at or below seq %d the copy checked, but the copy never applied it", describeOwn(now), now.Seq, o.upTo)
	case now.Seq == o.last.Seq && now.Seq > 0 && !now.Same(o.last):
		return differs(o.last, now)
	case now.Seq < o.last.Seq && tip.Horizon <= o.last.Seq:
		return fmt.Errorf("the shard's change at seq %d that the copy applied (%s) is gone, and the shard is pruned only below %d", o.last.Seq, describeOwn(o.last), tip.Horizon)
	}
	return nil
}

// describeOwn names a change of the copy's own shard.
func describeOwn(m store.ChangeMark) string {
	return fmt.Sprintf("%s %q", m.Kind, m.ID)
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
	ownCause := rc.ownRewound(tip, sh.IndexUID())
	switch {
	case tip.Head < from && rc.behind != from:
		rc.behind = from
		t.log.WarnContext(ctx, "the changelog's head is behind this copy's seq; polling again to tell a lagging read from a rewind",
			slog.Int64("seq", from), slog.Int64("head", tip.Head))
		return true, nil
	case tip.Head < from:
		cause = fmt.Errorf("the changelog's head %d is behind the copy's seq %d on two polls", tip.Head, from)
	case ownCause != nil:
		cause = ownCause
	case rc.anchor.Seq > 0 && tip.AtMark.Seq > 0 && !rc.anchor.Same(tip.AtMark):
		cause = differs(rc.anchor, tip.AtMark)
	case rc.anchor.Seq == 0 && rc.pending.Seq == from && tip.AtMark.Seq == from && !rc.pending.Same(tip.AtMark):
		cause = differs(rc.pending, tip.AtMark)
	default:
		rc.behind = 0
		rc.promoteOwn(sh, from, tip)
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

// promoteOwn makes the copy's pending own-shard state its checked one, once a poll from
// from (its applied seq, which the pending state is as of) found no rewind. A copy
// that knows nothing of its own shard (its manifest predates own marks) takes it from
// tip, which was read up to from, as pending.
func (rc *rewindCheck) promoteOwn(sh *shard.Shard, from int64, tip *store.Tip) {
	switch {
	case rc.ownPending.upTo == from && from > 0:
		rc.own, rc.ownPending = rc.ownPending, ownCheck{}
	case rc.own.upTo == 0 && rc.ownPending.upTo == 0 && from > 0:
		rc.ownPending = ownCheck{last: tip.ShardMark, upTo: from}
	default:
		return
	}
	sh.SetOwnMark(encodeOwn(rc.own, rc.ownPending))
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
