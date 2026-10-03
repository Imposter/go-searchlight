package shard

import "errors"

// Kill points: where a test's hook can simulate a crash (or block, or fail) inside a
// refresh, a merge or a commit.
const (
	// pointRefreshBuilt: the refresh's segment file is written, nothing is committed.
	pointRefreshBuilt = "refresh.built"
	// pointMergeBuilt: the merged segment is written, nothing is committed.
	pointMergeBuilt = "merge.built"
	// pointCommitSidecars: a commit's deletes sidecars are written, its manifest is not.
	pointCommitSidecars = "commit.sidecars"
	// pointManifestWritten: manifest.tmp is written and fsynced, not yet renamed.
	pointManifestWritten = "manifest.written"
	// pointManifestRenamed: the new manifest is renamed into place, the directory is
	// not yet fsynced, and nothing is published.
	pointManifestRenamed = "manifest.renamed"
)

// errSimulatedCrash, returned by a hook, makes the shard behave as if the process died
// at that point: the operation stops without cleaning up after itself (files stay as
// they are), and the shard fails. The test then abandons it and reopens the directory.
var errSimulatedCrash = errors.New("shard: simulated crash")

// testHooks are test-only seams.
type testHooks struct {
	// at is called at each kill point; an error aborts the operation there.
	at func(point string) error
	// remove replaces os.Remove for the janitor and garbage collection.
	remove func(path string) error
}

func (s *Shard) hook(point string) error {
	if s.opts.hooks == nil || s.opts.hooks.at == nil {
		return nil
	}
	err := s.opts.hooks.at(point)
	if isCrash(err) {
		s.fail(err)
	}
	return err
}
