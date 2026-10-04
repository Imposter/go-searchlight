package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// registry implements RegistryStore. A shard copy is a slot row of
// sl_shard_copies keyed by (index, shard, slot): claiming is a conditional
// insert-or-steal of a slot, so concurrent allocators can never place more
// copies than the target without any lock beyond the row's own.
type registry struct{ s *sqlStore }

func validNode(id string) error { return validKey("node id", id, MaxNodeID) }

func ttlMillis(ttl time.Duration) (int64, error) {
	if ttl < time.Millisecond {
		return 0, invalidf("lease ttl %s", ttl)
	}
	return ttl.Milliseconds(), nil
}

func (g *registry) Heartbeat(ctx context.Context, n Node) (err error) {
	s := g.s
	ctx, end := s.start(ctx, "heartbeat", attribute.String("node_id", n.ID))
	ctx = withHighLane(ctx)
	defer end(&err)
	if err := validNode(n.ID); err != nil {
		return err
	}
	if n.Capacity < 0 {
		return invalidf("capacity %d", n.Capacity)
	}
	_, err = s.w.ExecContext(ctx, s.d.Registry.Heartbeat, n.ID, n.Address, n.Version, n.Capacity)
	return err
}

func (g *registry) RemoveNode(ctx context.Context, nodeID string) (err error) {
	s := g.s
	ctx, end := s.start(ctx, "remove_node", attribute.String("node_id", nodeID))
	ctx = withHighLane(ctx)
	defer end(&err)
	if err := validNode(nodeID); err != nil {
		return err
	}
	_, err = s.w.ExecContext(ctx, s.d.Registry.RemoveNode, nodeID)
	return err
}

func (g *registry) Nodes(ctx context.Context) (out []Node, err error) {
	s := g.s
	ctx, end := s.start(ctx, "nodes")
	defer end(&err)
	rows, err := s.r.QueryContext(ctx, s.d.Registry.Nodes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var n Node
		var hb, started, now int64
		if err := rows.Scan(&n.ID, &n.Address, &n.Version, &n.Capacity, &hb, &started, &now); err != nil {
			return nil, err
		}
		n.HeartbeatAt, n.StartedAt = millis(hb), millis(started)
		n.HeartbeatAge = time.Duration(now-hb) * time.Millisecond
		out = append(out, n)
	}
	return out, rows.Err()
}

// scanCopy reads the dialect's CopyColumns.
func (g *registry) scanCopy(row interface{ Scan(...any) error }) (Copy, error) {
	var c Copy
	var state string
	var until, now int64
	err := row.Scan(&c.Shard.Index, &c.Shard.Shard, &c.Slot, &c.NodeID, &state, &c.AppliedSeq, &c.Epoch, &until, &now)
	c.State = CopyState(state)
	c.LeaseUntil = millis(until)
	c.LeaseLeft = time.Duration(until-now) * time.Millisecond
	return c, err
}

func (g *registry) ClaimCopy(ctx context.Context, shard ShardID, nodeID string, target int, ttl time.Duration) (c Copy, ok bool, err error) {
	s := g.s
	ctx, end := s.start(ctx, "claim_copy", attribute.String("index", shard.Index), attribute.Int("shard", shard.Shard),
		attribute.String("node_id", nodeID))
	ctx = withHighLane(ctx)
	defer end(&err)
	if err := validShard(shard); err != nil {
		return c, false, err
	}
	if err := validNode(nodeID); err != nil {
		return c, false, err
	}
	if target < 1 {
		return c, false, invalidf("target %d", target)
	}
	ms, err := ttlMillis(ttl)
	if err != nil {
		return c, false, err
	}
	for attempt := 1; ; attempt++ {
		c, ok, err = g.claimOnce(ctx, shard, nodeID, target, ms)
		if err == nil || attempt == applyAttempts || !s.retryable(err) || ctx.Err() != nil {
			return c, ok, err
		}
	}
}

func (g *registry) claimOnce(ctx context.Context, shard ShardID, nodeID string, target int, ttlMs int64) (Copy, bool, error) {
	s := g.s
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return Copy{}, false, err
	}
	defer rollback(tx)

	var indexed int
	if err := tx.QueryRowContext(ctx, s.d.Registry.IndexExists, shard.Index).Scan(&indexed); err != nil {
		return Copy{}, false, err
	}
	if indexed == 0 {
		return Copy{}, false, fmt.Errorf("index %q: %w", shard.Index, ErrNotFound)
	}

	rows, err := tx.QueryContext(ctx, s.d.Registry.Slots, shard.Index, shard.Shard)
	if err != nil {
		return Copy{}, false, err
	}
	var existing []Copy
	for rows.Next() {
		c, err := g.scanCopy(rows)
		if err != nil {
			rows.Close()
			return Copy{}, false, err
		}
		existing = append(existing, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Copy{}, false, err
	}

	// A node holds at most one copy of a shard: re-claim it if it has one.
	var candidates []int
	live := 0
	taken := make(map[int]bool, len(existing))
	for i := range existing {
		e := &existing[i]
		if e.NodeID == nodeID {
			candidates = []int{e.Slot}
			break
		}
		if !e.Expired() {
			live++
			taken[e.Slot] = true
		}
	}
	if candidates == nil {
		if live >= target {
			return Copy{}, false, nil
		}
		for slot := 0; slot < target; slot++ {
			if !taken[slot] {
				candidates = append(candidates, slot)
			}
		}
	}

	// A fresh fencing token for a new owner; a node renewing its own slot
	// keeps the epoch it has.
	epoch, err := g.nextEpoch(ctx, tx)
	if err != nil {
		return Copy{}, false, err
	}
	for _, slot := range candidates {
		// The claim locks the slot row; what it reads back is who holds the
		// slot now (no row: the slot was left to its live owner).
		row, err := returningRow(ctx, tx, s.d.Registry.Claim,
			[]any{shard.Index, shard.Shard, slot, nodeID, ttlMs, epoch}, []any{shard.Index, shard.Shard, slot})
		if err != nil {
			return Copy{}, false, fmt.Errorf("claim slot %d: %w", slot, err)
		}
		c, err := g.scanCopy(row)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			return Copy{}, false, fmt.Errorf("claim slot %d: %w", slot, err)
		case c.NodeID == nodeID:
			return c, true, tx.Commit()
		}
	}
	return Copy{}, false, tx.Commit()
}

// nextEpoch takes the next value of the epoch counter (sl_counter row 2),
// which is separate from the changelog sequence so claims never leave gaps
// in it.
func (g *registry) nextEpoch(ctx context.Context, tx *sql.Tx) (int64, error) {
	row, err := returningRow(ctx, tx, g.s.d.Registry.NextEpoch, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("next epoch: %w", err)
	}
	var epoch int64
	if err := row.Scan(&epoch); err != nil {
		return 0, fmt.Errorf("next epoch: %w", err)
	}
	return epoch, nil
}

func (g *registry) RenewLeases(ctx context.Context, nodeID string, ttl time.Duration) (out []ShardID, err error) {
	s := g.s
	ctx, end := s.start(ctx, "renew_leases", attribute.String("node_id", nodeID))
	ctx = withHighLane(ctx)
	defer end(&err)
	if err := validNode(nodeID); err != nil {
		return nil, err
	}
	ms, err := ttlMillis(ttl)
	if err != nil {
		return nil, err
	}
	// A renewal that reads back in its own statement is atomic as it is;
	// a write and a separate read take a transaction.
	var q queryer = s.w
	var tx *sql.Tx
	if s.d.Registry.Renew.Write != "" {
		if tx, err = s.w.BeginTx(ctx, s.d.ApplyTx); err != nil {
			return nil, err
		}
		defer rollback(tx)
		q = tx
	}
	rows, err := returning(ctx, q, s.d.Registry.Renew, []any{ms, nodeID}, []any{nodeID})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id ShardID
		if err := rows.Scan(&id.Index, &id.Shard); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b ShardID) int {
		if c := strings.Compare(a.Index, b.Index); c != 0 {
			return c
		}
		return a.Shard - b.Shard
	})
	if tx != nil {
		return out, tx.Commit()
	}
	return out, nil
}

func fenceArgs(c *Copy) []any { return []any{c.Shard.Index, c.Shard.Shard, c.Slot, c.NodeID, c.Epoch} }

func copyAttrs(c *Copy) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("index", c.Shard.Index), attribute.Int("shard", c.Shard.Shard),
		attribute.String("node_id", c.NodeID), attribute.Int64("epoch", c.Epoch),
	}
}

func (g *registry) ReleaseCopy(ctx context.Context, c Copy) (err error) {
	s := g.s
	ctx, end := s.start(ctx, "release_copy", copyAttrs(&c)...)
	ctx = withHighLane(ctx)
	defer end(&err)
	res, err := s.w.ExecContext(ctx, s.d.Registry.Release, fenceArgs(&c)...)
	return leaseResult(res, err, &c)
}

func (g *registry) Copies(ctx context.Context, index string) (out []Copy, err error) {
	s := g.s
	ctx, end := s.start(ctx, "copies", attribute.String("index", index))
	defer end(&err)
	q, args := s.d.Registry.Copies, []any(nil)
	if index != "" {
		q, args = s.d.Registry.IndexCopies, []any{index}
	}
	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		c, err := g.scanCopy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (g *registry) SetCopyState(ctx context.Context, c Copy, state CopyState) (err error) {
	s := g.s
	ctx, end := s.start(ctx, "set_copy_state", append(copyAttrs(&c), attribute.String("state", string(state)))...)
	ctx = withHighLane(ctx)
	defer end(&err)
	if err := validShard(c.Shard); err != nil {
		return err
	}
	if err := validNode(c.NodeID); err != nil {
		return err
	}
	if !state.valid() {
		return invalidf("copy state %q", state)
	}
	res, err := s.w.ExecContext(ctx, s.d.Registry.SetState, append([]any{string(state)}, fenceArgs(&c)...)...)
	return leaseResult(res, err, &c)
}

// ReportApplied is monotonic: a late report naming an older seq than one
// already recorded never moves applied_seq backwards.
func (g *registry) ReportApplied(ctx context.Context, c Copy, seq int64) (err error) {
	s := g.s
	ctx, end := s.start(ctx, "report_applied", copyAttrs(&c)...)
	defer end(&err)
	if err := validShard(c.Shard); err != nil {
		return err
	}
	if err := validNode(c.NodeID); err != nil {
		return err
	}
	res, err := s.w.ExecContext(ctx, s.d.Registry.ReportApplied, append([]any{seq}, fenceArgs(&c)...)...)
	return leaseResult(res, err, &c)
}

// leaseResult turns an update that matched no row into ErrLeaseLost. Every
// dialect reports matched rows (MySQL through CLIENT_FOUND_ROWS).
func leaseResult(res sql.Result, err error, c *Copy) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%s slot %d on node %q at epoch %d: %w", c.Shard, c.Slot, c.NodeID, c.Epoch, ErrLeaseLost)
	}
	return nil
}
