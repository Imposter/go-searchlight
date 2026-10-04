package api_test

import "testing"

// TestSuitesOnSingleNodeCluster runs the API suites again over a cluster of one
// (cluster.Node, with its registry, leases and allocator): it must serve every endpoint
// exactly as the single-node coordinator does.
func TestSuitesOnSingleNodeCluster(t *testing.T) {
	suites := []struct {
		name string
		fn   func(*testing.T)
	}{
		{"IndexLifecycle", TestIndexLifecycle},
		{"Documents", TestDocuments},
		{"ReadYourWrites", TestReadYourWrites},
		{"PatchMappingIndexesKeptFields", TestPatchMappingIndexesKeptFields},
		{"Bulk", TestBulk},
		{"Search", TestSearch},
		{"SavedQueriesAndPercolate", TestSavedQueriesAndPercolate},
		{"BulkPercolateMatchesBruteForce", TestBulkPercolateMatchesBruteForce},
		{"FieldsAndCluster", TestFieldsAndCluster},
		{"HostileRequests", TestHostileRequests},
		{"Auth", TestAuth},
		{"Backpressure", TestBackpressure},
		{"ShardBackpressureAndSearchQueueAre429", TestShardBackpressureAndSearchQueueAre429},
		{"RequestDeadline", TestRequestDeadline},
		{"WaitForSeqTimesOut", TestWaitForSeqTimesOut},
		{"InvalidUTF8CannotPoisonTheChangelog", TestInvalidUTF8CannotPoisonTheChangelog},
	}
	clusterMode.Store(true)
	defer clusterMode.Store(false)
	for _, s := range suites {
		t.Run(s.name, s.fn)
	}
}
