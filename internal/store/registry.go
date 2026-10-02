package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
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
	defer end(&err)
	if err := validNode(n.ID); err != nil {
		return err
	}
	if n.Capacity < 0 {
		return invalidf("capacity %d", n.Capacity)
	}
	q := "INSERT INTO sl_nodes (node_id, address, version, capacity, heartbeat_at, started_at) VALUES (?, ?, ?, ?, " +
		s.d.Now + ", " + s.d.Now + ")" +
		s.d.Upsert([]string{"node_id"}, []string{"address", "version", "capacity", "heartbeat_at"})
	_, err = s.w.ExecContext(ctx, s.bind(q), n.ID, n.Address, n.Version, n.Capacity)
	return err
}

func (g *registry) RemoveNode(ctx context.Context, nodeID string) (err error) {
	s := g.s
	ctx, end := s.start(ctx, "remove_node", attribute.String("node_id", nodeID))
	defer end(&err)
	if err := validNode(nodeID); err != nil {
		return err
	}
	_, err = s.w.ExecContext(ctx, s.bind("DELETE FROM sl_nodes WHERE node_id = ?"), nodeID)
	return err
}

func (g *registry) Nodes(ctx context.Context) (out []Node, err error) {
	s := g.s
	ctx, end := s.start(ctx, "nodes")
	defer end(&err)
	q := s.bind("SELECT node_id, address, version, capacity, heartbeat_at, started_at, " + s.d.Now + " FROM sl_nodes ORDER BY node_id")
	rows, err := s.r.QueryContext(ctx, q)
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

const copyCols = "index_name, shard, slot, node_id, state, applied_seq, lease_until"

func (g *registry) scanCopy(row interface{ Scan(...any) error }) (Copy, error) {
	var c Copy
	var state string
	var until, now int64
	err := row.Scan(&c.Shard.Index, &c.Shard.Shard, &c.Slot, &c.NodeID, &state, &c.AppliedSeq, &until, &now)
	c.State = CopyState(state)
	c.LeaseUntil = millis(until)
	c.LeaseLeft = time.Duration(until-now) * time.Millisecond
	return c, err
}

func (g *registry) ClaimCopy(ctx context.Context, shard ShardID, nodeID string, target int, ttl time.Duration) (c Copy, ok bool, err error) {
	s := g.s
	ctx, end := s.start(ctx, "claim_copy", attribute.String("index", shard.Index), attribute.Int("shard", shard.Shard),
		attribute.String("node_id", nodeID))
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

	slotQ := s.bind("SELECT " + copyCols + ", " + s.d.Now + " FROM sl_shard_copies WHERE index_name = ? AND shard = ? ORDER BY slot")
	rows, err := tx.QueryContext(ctx, slotQ, shard.Index, shard.Shard)
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

	ownQ := s.bind("SELECT " + copyCols + ", " + s.d.Now + " FROM sl_shard_copies WHERE index_name = ? AND shard = ? AND slot = ?")
	for _, slot := range candidates {
		q, args := s.d.Claim(dialect.ClaimArgs{Index: shard.Index, Shard: shard.Shard, Slot: slot, Node: nodeID, TTLms: ttlMs})
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return Copy{}, false, fmt.Errorf("claim slot %d: %w", slot, err)
		}
		// The claim statement locked the slot row; read who holds it now.
		c, err := g.scanCopy(tx.QueryRowContext(ctx, ownQ, shard.Index, shard.Shard, slot))
		if err != nil {
			return Copy{}, false, err
		}
		if c.NodeID == nodeID {
			return c, true, tx.Commit()
		}
	}
	return Copy{}, false, tx.Commit()
}

func (g *registry) RenewLeases(ctx context.Context, nodeID string, ttl time.Duration) (out []ShardID, err error) {
	s := g.s
	ctx, end := s.start(ctx, "renew_leases", attribute.String("node_id", nodeID))
	defer end(&err)
	if err := validNode(nodeID); err != nil {
		return nil, err
	}
	ms, err := ttlMillis(ttl)
	if err != nil {
		return nil, err
	}
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	upd := s.bind("UPDATE sl_shard_copies SET lease_until = " + s.d.Now + " + ? WHERE node_id = ? AND lease_until >= " + s.d.Now)
	if _, err := tx.ExecContext(ctx, upd, ms, nodeID); err != nil {
		return nil, err
	}
	sel := s.bind("SELECT index_name, shard FROM sl_shard_copies WHERE node_id = ? AND lease_until >= " + s.d.Now +
		" ORDER BY index_name, shard")
	rows, err := tx.QueryContext(ctx, sel, nodeID)
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
	return out, tx.Commit()
}

func (g *registry) ReleaseCopy(ctx context.Context, shard ShardID, nodeID string) (err error) {
	s := g.s
	ctx, end := s.start(ctx, "release_copy", attribute.String("index", shard.Index), attribute.Int("shard", shard.Shard),
		attribute.String("node_id", nodeID))
	defer end(&err)
	if err := validShard(shard); err != nil {
		return err
	}
	_, err = s.w.ExecContext(ctx, s.bind("DELETE FROM sl_shard_copies WHERE index_name = ? AND shard = ? AND node_id = ?"),
		shard.Index, shard.Shard, nodeID)
	return err
}

func (g *registry) Copies(ctx context.Context, index string) (out []Copy, err error) {
	s := g.s
	ctx, end := s.start(ctx, "copies", attribute.String("index", index))
	defer end(&err)
	q := "SELECT " + copyCols + ", " + s.d.Now + " FROM sl_shard_copies"
	var args []any
	if index != "" {
		q += " WHERE index_name = ?"
		args = append(args, index)
	}
	rows, err := s.r.QueryContext(ctx, s.bind(q+" ORDER BY index_name, shard, slot"), args...)
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

func (g *registry) SetCopyState(ctx context.Context, shard ShardID, nodeID string, state CopyState) (err error) {
	s := g.s
	ctx, end := s.start(ctx, "set_copy_state", attribute.String("index", shard.Index), attribute.Int("shard", shard.Shard),
		attribute.String("node_id", nodeID), attribute.String("state", string(state)))
	defer end(&err)
	if !state.valid() {
		return invalidf("copy state %q", state)
	}
	q := s.bind("UPDATE sl_shard_copies SET state = ? WHERE index_name = ? AND shard = ? AND node_id = ? AND lease_until >= " + s.d.Now)
	res, err := s.w.ExecContext(ctx, q, string(state), shard.Index, shard.Shard, nodeID)
	return leaseResult(res, err, shard, nodeID)
}

func (g *registry) ReportApplied(ctx context.Context, shard ShardID, nodeID string, seq int64) (err error) {
	s := g.s
	ctx, end := s.start(ctx, "report_applied", attribute.String("index", shard.Index), attribute.Int("shard", shard.Shard),
		attribute.String("node_id", nodeID))
	defer end(&err)
	q := s.bind("UPDATE sl_shard_copies SET applied_seq = ? WHERE index_name = ? AND shard = ? AND node_id = ?")
	res, err := s.w.ExecContext(ctx, q, seq, shard.Index, shard.Shard, nodeID)
	return leaseResult(res, err, shard, nodeID)
}

// leaseResult turns an update that matched no row into ErrLeaseLost. Every
// dialect reports matched rows (MySQL through CLIENT_FOUND_ROWS).
func leaseResult(res sql.Result, err error, shard ShardID, nodeID string) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%s on node %q: %w", shard, nodeID, ErrLeaseLost)
	}
	return nil
}
