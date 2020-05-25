package aggregates

import (
	"context"
	"sync"
	"time"
)

type cacheItem struct {
	item interface{}
	deadline time.Time
}

var cacheItemPool = sync.Pool{New: func() interface{} {
	return &cacheItem{}
}}

func cacheItemAcquire(v interface{}, deadline time.Time) *cacheItem {
	item := cacheItemPool.Get().(*cacheItem)
	item.item = v
	item.deadline = deadline
	return item
}

func cacheItemRelease(item *cacheItem) {
	cacheItemPool.Put(item)
}

type cacheTTL struct {
	ttl time.Duration
	elements *sync.Map
}

func (c *cacheTTL) get(key string) (v interface{}, has bool) {
	i0, has0 := c.elements.Load(key)
	if !has0 {
		return
	}
	i, ok := i0.(*cacheItem)
	if !ok {
		c.elements.Delete(key)
		cacheItemRelease(i)
		return
	}
	v = i.item
	return
}

func (c *cacheTTL) put(key string, v interface{}, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	deadline := time.Now().Add(ttl)
	item := cacheItemAcquire(v, deadline)
	c.elements.Store(key, item)
}

func (c *cacheTTL) loopClean(ctx context.Context) {
	go func(ctx context.Context, c *cacheTTL) {
		stop := false
		for  {
			select {
			case <- ctx.Done():
				stop = true
			case <- time.After(c.ttl):
				keys := make([]interface{}, 0, 1)
				vals := make([]*cacheItem, 0, 1)
				c.elements.Range(func(key, value interface{}) bool {
					i, ok := value.(*cacheItem)
					if !ok {
						keys = append(keys, key)
						return true
					}
					if i.deadline.Before(time.Now()) {
						keys = append(keys, key)
						vals = append(vals, i)
						return true
					}
					return true
				})
				for _, key := range keys {
					c.elements.Delete(key)
				}
				for _, val := range vals {
					cacheItemRelease(val)
				}
			}
			if stop {
				break
			}
		}
	}(ctx, c)
}
