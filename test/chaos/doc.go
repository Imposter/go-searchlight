//go:build chaos

// Package chaos is Searchlight's chaos suite (plan Task 13, spec section 14): real
// searchlight processes (internal/slproc) in a three-node cluster on Postgres, under a
// write and read load sent through a load balancer that fails over between nodes,
// while nodes are killed mid-bulk, mid-merge and mid-recovery, the database restarts, a
// segment is corrupted on disk, and the nodes are upgraded one at a time. After each
// scenario the spec's invariants are checked:
//
//   - no acknowledged write is lost: every node serves exactly the documents the
//     acknowledged writes left, at their last acknowledged versions;
//   - no client-visible error with two or more copies, apart from the documented
//     retryable cases (a node gone: the balancer fails over; 429 and 503: retried);
//   - the copies converge: every copy holds its shard's documents, and every node
//     answers the same queries alike;
//   - a corrupt segment is detected and its copy rebuilt, never served;
//   - a node's restart time does not grow with the index's size.
//
// The tests need the build tag (go test -tags chaos ./test/chaos) and a Postgres in
// SEARCHLIGHT_TEST_PG_URL; without one they skip. The database restart scenario also
// needs SEARCHLIGHT_CHAOS_DB_RESTART, a shell command that restarts that Postgres (in
// CI, docker restart of its service container). CI runs the suite in
// .github/workflows/chaos.yml.
package chaos
