package api

import (
	"os"
	"regexp"
	"strconv"
	"testing"
)

// TestNginxFitsTheLargestToken: the shipped nginx configuration's request line buffer
// holds a request carrying the largest token as wait_for, with room for the path and
// other parameters, so nginx never answers such a read with its own 414.
func TestNginxFitsTheLargestToken(t *testing.T) {
	conf, err := os.ReadFile("../../deploy/nginx/searchlight.conf")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*large_client_header_buffers\s+\d+\s+(\d+)k;`).FindSubmatch(conf)
	if m == nil {
		t.Fatal("deploy/nginx/searchlight.conf sets no large_client_header_buffers: nginx's default 8k is shorter than the largest token")
	}
	kib, err := strconv.Atoi(string(m[1]))
	if err != nil {
		t.Fatal(err)
	}
	const room = 4 << 10 // the path, the other parameters and the HTTP version
	if got, want := kib<<10, MaxTokenBytes+room; got < want {
		t.Fatalf("large_client_header_buffers is %d bytes, below the largest token's %d plus %d of room", got, MaxTokenBytes, room)
	}
}
