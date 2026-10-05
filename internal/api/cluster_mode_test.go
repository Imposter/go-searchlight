package api_test

import (
	"testing"

	"github.com/Imposter/go-searchlight/internal/testtier"
)

// TestSuitesOnSingleNodeCluster runs the API suites again over a cluster of one
// (cluster.Node, with its registry, leases and allocator): it must serve every endpoint
// exactly as the single-node coordinator does. The suites that wait out real deadlines
// and backoffs run in the heavy tier only.
func TestSuitesOnSingleNodeCluster(t *testing.T) {
	suites := []struct {
		name  string
		fn    func(*testing.T)
		heavy bool
	}{
		{"IndexLifecycle", TestIndexLifecycle, false},
		{"Documents", TestDocuments, false},
		{"ReadYourWrites", TestReadYourWrites, true},
		{"PatchMappingIndexesKeptFields", TestPatchMappingIndexesKeptFields, false},
		{"Bulk", TestBulk, false},
		{"Search", TestSearch, false},
		{"SavedQueriesAndPercolate", TestSavedQueriesAndPercolate, false},
		{"BulkPercolateMatchesBruteForce", TestBulkPercolateMatchesBruteForce, false},
		{"FieldsAndCluster", TestFieldsAndCluster, false},
		{"HostileRequests", TestHostileRequests, false},
		{"Auth", TestAuth, false},
		{"Backpressure", TestBackpressure, true},
		{"ShardBackpressureAndSearchQueueAre429", TestShardBackpressureAndSearchQueueAre429, false},
		{"RequestDeadline", TestRequestDeadline, false},
		{"WaitForSeqTimesOut", TestWaitForSeqTimesOut, true},
		{"InvalidUTF8CannotPoisonTheChangelog", TestInvalidUTF8CannotPoisonTheChangelog, false},
	}
	clusterMode.Store(true)
	defer clusterMode.Store(false)
	for _, s := range suites {
		t.Run(s.name, func(t *testing.T) {
			if s.heavy {
				testtier.Heavy(t)
			}
			s.fn(t)
		})
	}
}
