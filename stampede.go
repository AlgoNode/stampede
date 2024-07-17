package stampede

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/goware/singleflight"
	lru "github.com/hashicorp/golang-lru/v2"
)

// Prevents cache stampede https://en.wikipedia.org/wiki/Cache_stampede by only running a
// single data fetch operation per expired / missing key regardless of number of requests to that key.

func NewCache(size int, ttl, errorTtl time.Duration) *Cache[any] {
	return NewCacheKV[any](size, ttl, errorTtl)
}

func NewCacheKV[K comparable](size int, ttl, errorTtl time.Duration) *Cache[K] {
	values, _ := lru.New[K, value](size)
	return &Cache[K]{
		ttl:      ttl,
		errorTtl: errorTtl,
		values:   values,
	}
}

type Cache[K comparable] struct {
	values *lru.Cache[K, value]

	ttl      time.Duration
	errorTtl time.Duration

	mu        sync.RWMutex
	callGroup singleflight.Group[K, *responseValue]
}

func (c *Cache[K]) Get(ctx context.Context, key K, fn singleflight.DoFunc[*responseValue]) (*responseValue, error) {
	return c.get(ctx, key, false, fn)
}

func (c *Cache[K]) GetFresh(ctx context.Context, key K, fn singleflight.DoFunc[*responseValue]) (*responseValue, error) {
	return c.get(ctx, key, true, fn)
}

func (c *Cache[K]) Set(ctx context.Context, key K, fn singleflight.DoFunc[*responseValue]) (*responseValue, bool, error) {
	v, err, shared := c.callGroup.Do(key, c.set(key, fn))
	return v, shared, err
}

func (c *Cache[K]) get(ctx context.Context, key K, freshOnly bool, fn singleflight.DoFunc[*responseValue]) (*responseValue, error) {
	c.mu.RLock()
	val, ok := c.values.Get(key)
	c.mu.RUnlock()

	// value exists and is fresh - just return
	if ok && val.IsFresh() {
		return val.Value(), nil
	}

	// value exists and is stale, and we're OK with serving it stale while updating in the background
	// note: stale means its still okay, but not fresh. but if its expired, then it means its useless.
	if ok && !freshOnly && !val.IsExpired() {
		// TODO: technically could be a stampede of goroutines here if the value is expired
		// and we're OK with serving it stale
		go c.Set(ctx, key, fn)
		return val.Value(), nil
	}

	// value doesn't exist or is expired, or is stale and we need it fresh (freshOnly:true) - sync update
	v, _, err := c.Set(ctx, key, fn)
	return v, err
}

func (c *Cache[K]) set(key K, fn singleflight.DoFunc[*responseValue]) singleflight.DoFunc[*responseValue] {
	return singleflight.DoFunc[*responseValue](func() (*responseValue, error) {
		val, err := fn()
		if err != nil {
			return val, err
		}

		var effectiveTtl time.Duration
		if val.status == http.StatusOK {
			effectiveTtl = c.ttl
		} else {
			effectiveTtl = c.errorTtl
		}

		c.mu.Lock()
		c.values.Add(key, value{
			v:          val,
			expiry:     time.Now().Add(effectiveTtl * 2),
			bestBefore: time.Now().Add(effectiveTtl),
		})
		c.mu.Unlock()

		return val, nil
	})
}

type value struct {
	v *responseValue

	bestBefore time.Time // cache entry freshness cutoff
	expiry     time.Time // cache entry time to live cutoff
}

func (v *value) IsFresh() bool {
	return v.bestBefore.After(time.Now())
}

func (v *value) IsExpired() bool {
	return v.expiry.Before(time.Now())
}

func (v *value) Value() *responseValue {
	return v.v
}

func BytesToHash(b ...[]byte) uint64 {
	d := xxhash.New()
	for _, v := range b {
		d.Write(v)
	}
	return d.Sum64()
}

func StringToHash(s ...string) uint64 {
	d := xxhash.New()
	for _, v := range s {
		d.WriteString(v)
	}
	return d.Sum64()
}
