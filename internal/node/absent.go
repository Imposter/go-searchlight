package node

import (
	"sync"
	"time"
)

// absentTTL is how long a cluster node answers 404 for an index name the store did not
// have without asking it again. An index another node creates meanwhile is found once
// it passes, or at the next catalogue sync, whichever comes first.
const absentTTL = 250 * time.Millisecond

const absentMax = 4096

// absentIndexes are the index names the store recently did not have. gen counts the
// names forgotten, so a lookup that raced a create does not record its stale answer.
type absentIndexes struct {
	mu    sync.Mutex
	names map[string]time.Time
	gen   uint64
}

func (a *absentIndexes) check(name string) (gen uint64, absent bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	at, ok := a.names[name]
	return a.gen, ok && time.Since(at) < absentTTL
}

func (a *absentIndexes) note(name string, gen uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if gen != a.gen {
		return
	}
	if a.names == nil {
		a.names = map[string]time.Time{}
	}
	if len(a.names) >= absentMax {
		for k, at := range a.names {
			if time.Since(at) >= absentTTL {
				delete(a.names, k)
			}
		}
		if len(a.names) >= absentMax {
			return
		}
	}
	a.names[name] = time.Now()
}

func (a *absentIndexes) forget(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.names, name)
	a.gen++
}
