package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

type warmFake struct{ closed bool }

func (f *warmFake) Close() error { f.closed = true; return nil }

func TestSessionCache(t *testing.T) {
	now := time.Unix(0, 0)
	c := &SessionCache{TTL: 30 * time.Minute, Max: 2, now: func() time.Time { return now }}
	opens := 0
	open := func(context.Context) (adapter.Session, error) { opens++; return &warmFake{}, nil }
	ctx := context.Background()

	s, _ := c.Get(ctx, "a", open)
	c.Put("a", s)
	if s2, _ := c.Get(ctx, "a", open); s2 != s || opens != 1 {
		t.Fatal("warm session not reused")
	}
	c.Put("a", s)
	now = now.Add(31 * time.Minute)
	if s3, _ := c.Get(ctx, "a", open); s3 == s || opens != 2 || !s.(*warmFake).closed {
		t.Fatal("expired session reused or not closed")
	}
	// max eviction: oldest closed
	x, y, z := &warmFake{}, &warmFake{}, &warmFake{}
	c.Put("x", x)
	now = now.Add(time.Second)
	c.Put("y", y)
	now = now.Add(time.Second)
	c.Put("z", z)
	if !x.closed || y.closed || z.closed {
		t.Fatal("oldest not evicted")
	}
}
