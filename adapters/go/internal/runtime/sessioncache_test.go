package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

type warmFake struct{ closed atomic.Bool }

func (f *warmFake) Close() error { f.closed.Store(true); return nil }
func (f *warmFake) KeepOpen()    {}

// singleFake hides KeepOpen: a non-reusable session.
type singleFake struct{ f *warmFake }

func (s singleFake) Close() error { return s.f.Close() }

func TestSessionCache(t *testing.T) {
	var mu sync.Mutex
	now := time.Unix(0, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	c := &SessionCache{TTL: 30 * time.Minute, Max: 2, now: clock}
	var opens atomic.Int32
	open := func(context.Context) (adapter.Session, error) {
		opens.Add(1)
		time.Sleep(20 * time.Millisecond)
		return &warmFake{}, nil
	}
	ctx := context.Background()

	// concurrent requests share one session, opened once
	got := make([]adapter.Session, 20)
	var wg sync.WaitGroup
	for i := range got {
		wg.Add(1)
		go func() { defer wg.Done(); got[i], _ = c.Get(ctx, "a", open) }()
	}
	wg.Wait()
	for _, s := range got {
		if s != got[0] {
			t.Fatal("concurrent gets returned different sessions")
		}
	}
	if opens.Load() != 1 {
		t.Fatalf("opened %d times, want 1", opens.Load())
	}

	advance(31 * time.Minute)
	s2, _ := c.Get(ctx, "a", open)
	if s2 == got[0] || opens.Load() != 2 || !got[0].(*warmFake).closed.Load() {
		t.Fatal("expired session reused or not closed")
	}

	// max eviction: oldest closed
	advance(time.Second) // distinct timestamps: "oldest" must be unambiguous
	b, _ := c.Get(ctx, "b", open)
	advance(time.Second)
	cc, _ := c.Get(ctx, "c", open)
	if !s2.(*warmFake).closed.Load() || b.(*warmFake).closed.Load() || cc.(*warmFake).closed.Load() {
		t.Fatal("oldest not evicted")
	}
}

func TestSessionCacheSingleUseNotShared(t *testing.T) {
	c := &SessionCache{TTL: time.Minute, Max: 2}
	var opens atomic.Int32
	open := func(context.Context) (adapter.Session, error) {
		opens.Add(1)
		time.Sleep(10 * time.Millisecond)
		return singleFake{&warmFake{}}, nil
	}
	var wg sync.WaitGroup
	seen := make([]adapter.Session, 5)
	for i := range seen {
		wg.Add(1)
		go func() { defer wg.Done(); seen[i], _ = c.Get(context.Background(), "k", open) }()
	}
	wg.Wait()
	if opens.Load() != 5 {
		t.Fatalf("single-use sessions must not be shared: opens=%d", opens.Load())
	}
}
