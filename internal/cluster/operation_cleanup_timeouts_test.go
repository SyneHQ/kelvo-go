package cluster

import (
	"testing"
	"time"
)

func TestCleanupTimeoutsLogFirstThenEverySixtiethAndResetOnSuccess(t *testing.T) {
	t0 := time.Unix(1000, 0)
	c := cleanupTimeouts{lastOK: t0}
	for i := 1; i <= 121; i++ {
		count, since, logIt := c.timedOut(t0.Add(time.Duration(i) * time.Second))
		if count != i || since != time.Duration(i)*time.Second || logIt != (i == 1 || i == 60 || i == 120) {
			t.Fatalf("timeout %d: count=%d since=%v log=%v", i, count, since, logIt)
		}
	}
	c.succeeded(t0.Add(200 * time.Second))
	count, since, logIt := c.timedOut(t0.Add(203 * time.Second))
	if count != 1 || since != 3*time.Second || !logIt {
		t.Fatalf("after success: count=%d since=%v log=%v", count, since, logIt)
	}
}
