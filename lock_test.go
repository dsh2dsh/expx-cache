package cache

import (
	"context"
	"errors"
	"iter"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/dsh2dsh/expx-cache-redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (self *CacheTestSuite) TestOnceLock() {
	tests := []struct {
		name        string
		cache2      func() *Cache
		assertCount func(callCount uint32)
	}{
		{
			name: "different caches",
			cache2: func() *Cache {
				self.expectCacheMiss()
				return self.newCache(self.withLock())
			},
			assertCount: func(callCount uint32) {
				if self.cache.redis != nil {
					self.Equal(uint32(1), callCount)
				} else {
					self.Equal(uint32(2), callCount)
				}
			},
		},
		{
			name: "same cache",
			cache2: func() *Cache {
				self.expectCacheMiss()
				self.stats.localHit()
				return self.cache
			},
			assertCount: func(callCount uint32) {
				self.Equal(uint32(1), callCount)
			},
		},
	}

	for _, tt := range tests {
		self.Run(tt.name, func() {
			self.TearDownTest()
			self.SetupTest()
			self.cache = self.cache.New(self.withLock())
			sig := make(chan struct{})
			var callCount uint32

			ctx, cancel := context.WithDeadline(context.Background(),
				time.Now().Add(time.Second))
			defer cancel()

			cache2 := tt.cache2()
			var got2 CacheableObject
			go func() {
				<-sig
				self.NoError(cache2.OnceLock(ctx, Item{
					Key:   testKey,
					Value: &got2,
					Do: func(ctx context.Context) (any, error) {
						atomic.AddUint32(&callCount, 1)
						return self.CacheableValue(), nil
					},
				}))
				close(sig)
			}()

			var got CacheableObject
			err := self.cache.OnceLock(ctx, Item{
				Key:   testKey,
				Value: &got,
				Do: func(ctx context.Context) (any, error) {
					atomic.AddUint32(&callCount, 1)
					sig <- struct{}{}
					time.Sleep(100 * time.Millisecond)
					return self.CacheableValue(), nil
				},
			})
			self.Require().NoError(err)

			<-sig
			self.Equal(self.CacheableValue(), got)
			self.Equal(&got, &got2)
			tt.assertCount(callCount)
			self.assertStats()
		})
	}
}

func (self *CacheTestSuite) withLock() Option {
	return WithLock(self.cache.cfgLock.TTL, self.cache.cfgLock.Tick,
		func() WaitLockIter { return NewWaitLockIter(60 * time.Millisecond) })
}

func (self *CacheTestSuite) TestOnceLock_error() {
	ctx := context.Background()
	wantErr := errors.New("test error")
	err := self.cache.OnceLock(ctx, Item{
		Key: testKey,
		Do: func(ctx context.Context) (any, error) {
			return nil, wantErr
		},
	})
	self.Require().ErrorIs(err, wantErr)
}

func (self *CacheTestSuite) TestOnceLock_disappearedLock() {
	self.needsRedis()

	ctx := context.Background()
	err := self.cache.Set(ctx, Item{
		Key:            self.cache.keyLocked(testKey),
		Value:          "some value",
		SkipLocalCache: true,
	})
	self.Require().NoError(err)

	waitIter := self.cache.cfgLock.Iter
	self.T().Cleanup(func() { self.cache.cfgLock.Iter = waitIter })

	var callCount int
	self.cache.cfgLock.Iter = func() WaitLockIter {
		return func() time.Duration {
			callCount++
			self.NoError(self.cache.Delete(ctx, self.cache.keyLocked(testKey)))
			return time.Millisecond
		}
	}

	const want = "foobar"
	var got string
	err = self.cache.OnceLock(ctx, Item{
		Key:   testKey,
		Value: &got,
		Do: func(ctx context.Context) (any, error) {
			callCount++
			return want, nil
		},
	})

	self.Require().NoError(err)
	self.Equal(2, callCount)
	self.Equal(want, got)
}

func (self *CacheTestSuite) TestLock_Get_notExists() {
	self.needsRedis()

	ctx, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(time.Second))
	defer cancel()

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey), testKey)
	b, err := l.Get(ctx, lockTTL, NewWaitLockIter(time.Second))
	self.Require().NoError(err)
	self.Nil(b)

	var gotLockVal string
	item := Item{
		Key:            self.cache.keyLocked(testKey),
		Value:          &gotLockVal,
		SkipLocalCache: true,
	}

	missed, err := self.cache.Get(ctx, item)
	self.Require().NoError(err)
	self.Nil(missed)
	self.Equal(l.value, gotLockVal)

	gotLockTTL, err := self.rdb.PTTL(ctx, self.cache.ResolveKeyLock(testKey)).Result()
	self.Require().NoError(err)
	self.NotZero(gotLockTTL)
	self.LessOrEqual(gotLockTTL, lockTTL)

	self.Require().NoError(l.release(ctx))
	missed, err = self.cache.Get(ctx, item)
	self.Require().NoError(err)
	self.NotEmpty(missed, "lock exists after release")
}

func (self *CacheTestSuite) TestLock_Get_ok() {
	self.needsRedis()

	ctx, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(time.Second))
	defer cancel()

	const foobar = "foobar"
	self.Require().NoError(self.cache.Set(ctx, Item{
		Key: testKey, Value: foobar, SkipLocalCache: true,
	}))

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey),
		self.cache.ResolveKey(testKey))
	b, err := l.Get(ctx, lockTTL, NewWaitLockIter(time.Second))
	self.Require().NoError(err)
	self.Equal([]byte(foobar), b)

	var gotLockVal string
	missed, err := self.cache.Get(ctx, Item{
		Key:            self.cache.keyLocked(testKey),
		Value:          &gotLockVal,
		SkipLocalCache: true,
	})
	self.Require().NoError(err)
	self.NotEmpty(missed, "lock exists after Get ok")
}

func (self *CacheTestSuite) TestLock_Get_okLockExists() {
	self.needsRedis()

	const foobar = "foobar"
	makeFreqIter := func() func() time.Duration {
		return NewWaitLockIter(time.Second)
	}

	ctx, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(time.Second))
	defer cancel()

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey),
		self.cache.ResolveKey(testKey))
	b, err := l.Get(ctx, lockTTL, makeFreqIter())
	self.Require().NoError(err)
	self.Nil(b)

	self.Require().NoError(self.cache.Set(ctx, Item{
		Key: testKey, Value: foobar, SkipLocalCache: true,
	}))

	l2 := self.cache.lock(self.cache.ResolveKeyLock(testKey),
		self.cache.ResolveKey(testKey))
	b, err = l2.Get(ctx, lockTTL, makeFreqIter())
	self.Require().NoError(err)
	self.Equal([]byte(foobar), b)

	var gotLockVal string
	missed, err := self.cache.Get(ctx, Item{
		Key:            self.cache.keyLocked(testKey),
		Value:          &gotLockVal,
		SkipLocalCache: true,
	})
	self.Require().NoError(err)
	self.Empty(missed, "lock doesn't exist")
}

func (self *CacheTestSuite) TestLock_Get_waitLockDeadline() {
	self.needsRedis()

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey),
		self.cache.ResolveKey(testKey))
	ctx := context.Background()
	b, err := l.Get(ctx, lockTTL, NewWaitLockIter(time.Second))
	self.Require().NoError(err)
	self.Nil(b)

	ctx2, cancel := context.WithDeadline(ctx, time.Now().Add(100*time.Millisecond))
	defer cancel()

	l2 := self.cache.lock(self.cache.ResolveKeyLock(testKey),
		self.cache.ResolveKey(testKey))
	_, err = l2.Get(ctx2, lockTTL, NewWaitLockIter(time.Second))
	self.T().Log(err)
	self.Require().ErrorIs(err, context.DeadlineExceeded)
	self.True(self.cache.Failed())
	self.True(self.cache.Unfail())

	var gotLockVal string
	missed, err := self.cache.Get(ctx, Item{
		Key:            self.cache.keyLocked(testKey),
		Value:          &gotLockVal,
		SkipLocalCache: true,
	})
	self.Require().NoError(err)
	self.Empty(missed, "lock doesn't exist")
	self.Equal(l.value, gotLockVal)

	wantErr := errors.New("test error")
	l2.notFound = func(key, value string) error {
		self.Equal(l2.keyLock, key)
		self.Equal(l2.value, value)
		return wantErr
	}
	self.Require().ErrorIs(l2.release(ctx), wantErr)
}

func (self *CacheTestSuite) TestLock_Get_withValue() {
	self.needsRedis()

	ctx, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(time.Second))
	defer cancel()

	const foobar = "foobar"
	self.cache.requestId = foobar
	l := self.cache.lock(self.cache.ResolveKeyLock(testKey),
		self.cache.ResolveKey(testKey))
	b, err := l.Get(ctx, lockTTL, NewWaitLockIter(time.Second))
	self.Require().NoError(err)
	self.Nil(b)

	var gotLockVal string
	missed, err := self.cache.Get(ctx, Item{
		Key:            self.cache.keyLocked(testKey),
		Value:          &gotLockVal,
		SkipLocalCache: true,
	})
	self.Require().NoError(err)
	self.Empty(missed, "lock doesn't exist")
	self.T().Log(gotLockVal)
	self.True(strings.HasPrefix(gotLockVal, foobar))
}

func (self *CacheTestSuite) TestLock_WithLock() {
	self.needsRedis()

	ctx, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(time.Second))
	defer cancel()

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey),
		self.cache.ResolveKey(testKey))
	b, err := l.Get(ctx, lockTTL, NewWaitLockIter(time.Second))
	self.Require().NoError(err)
	self.Nil(b)

	var callCount int
	err = l.WithLock(ctx, lockTTL, 30*time.Millisecond, func() error {
		callCount++
		var s string
		missed, err := self.cache.Get(ctx, Item{
			Key:            self.cache.keyLocked(testKey),
			Value:          &s,
			SkipLocalCache: true,
		})
		self.Require().NoError(err)
		self.Empty(missed)
		self.Equal(l.value, s)
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	self.Require().NoError(err)
	self.Equal(1, callCount)

	var s string
	missed, err := self.cache.Get(ctx, Item{
		Key:            self.cache.keyLocked(testKey),
		Value:          &s,
		SkipLocalCache: true,
	})
	self.Require().NoError(err)
	self.Len(missed, 1, "lock still exists")

	wantErr := errors.New("test error")
	l.notFound = func(key, value string) error {
		self.Equal(l.keyLock, key)
		self.Equal(l.value, value)
		return wantErr
	}
	err = l.WithLock(ctx, lockTTL, time.Second, func() error {
		callCount++
		return nil
	})
	self.Require().ErrorIs(err, wantErr)
	self.Equal(2, callCount)

	err = l.WithLock(ctx, lockTTL, time.Second, func() error {
		callCount++
		return wantErr
	})
	self.Require().ErrorIs(err, wantErr)
	self.Equal(3, callCount)
}

func TestNewWaitLockIter(t *testing.T) {
	tests := []struct {
		name       string
		start      time.Duration
		seq        []time.Duration
		N          int
		minTime    time.Duration
		maxTime    time.Duration
		beginsWith []string
	}{
		{
			name:       "3 1s",
			start:      time.Second,
			N:          3,
			minTime:    time.Second,
			maxTime:    time.Second,
			beginsWith: []string{"1s", "1s", "1s"},
		},
		{
			name:       "3 1s 10s",
			start:      time.Second,
			seq:        []time.Duration{10 * time.Second},
			N:          3,
			minTime:    time.Second,
			maxTime:    10 * time.Second,
			beginsWith: []string{"1s"},
		},
		{
			name:       "3 1s 1s",
			start:      time.Second,
			seq:        []time.Duration{time.Second},
			N:          3,
			minTime:    time.Second,
			maxTime:    time.Second,
			beginsWith: []string{"1s", "1s", "1s"},
		},
		{
			name:       "3 1s 1s,10s",
			start:      time.Second,
			seq:        []time.Duration{time.Second, 10 * time.Second},
			N:          3,
			minTime:    time.Second,
			maxTime:    10 * time.Second,
			beginsWith: []string{"1s", "1s"},
		},
		{
			name:       "3 1s 1s,1s,10s",
			start:      time.Second,
			seq:        []time.Duration{time.Second, time.Second, 10 * time.Second},
			N:          5,
			minTime:    time.Second,
			maxTime:    10 * time.Second,
			beginsWith: []string{"1s", "1s", "1s"},
		},
		{
			name:       "3 1s 1s,1s,1s,1s",
			start:      time.Second,
			seq:        []time.Duration{time.Second, time.Second, time.Second, time.Second},
			N:          5,
			minTime:    time.Second,
			maxTime:    10 * time.Second,
			beginsWith: []string{"1s", "1s", "1s", "1s", "1s"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := NewWaitLockIter(tt.start, tt.seq...)
			gotTimes := make([]time.Duration, tt.N)
			strTimes := make([]string, tt.N)
			for i := range tt.N {
				gotTimes[i] = iter()
				strTimes[i] = gotTimes[i].String()
			}
			t.Log(strTimes)
			if len(tt.beginsWith) > 0 {
				assert.Equal(t, tt.beginsWith, strTimes[:len(tt.beginsWith)])
			}
			for _, v := range gotTimes[len(tt.beginsWith):] {
				assert.GreaterOrEqual(t, v, tt.minTime)
				assert.LessOrEqual(t, v, tt.maxTime)
			}
		})
	}
}

func TestLock_Get_errorRandRead(t *testing.T) {
	cache := New()
	l := cache.lock(cache.ResolveKeyLock(testKey), cache.ResolveKey(testKey))

	wantErr := errors.New("test error")
	l.randRead = func(b []byte) (int, error) {
		return 0, wantErr
	}

	ctx := t.Context()
	b, err := l.Get(ctx, lockTTL, NewWaitLockIter(time.Second))
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, b)
}

func TestLock_releaseNoValue(t *testing.T) {
	var l lock
	require.ErrorContains(t, l.release(t.Context()), "empty value")
}

func TestLock_lockGet_getAfterSubscribe(t *testing.T) {
	ctx := t.Context()

	redisCache := &MoqRedisCache{}
	cache := New().WithRedisCache(redisCache)

	keyLock := cache.ResolveKeyLock(testKey)
	keyGet := cache.ResolveKey(testKey)
	const foobar = "foobar"

	redisCache.LockGetFunc = func(ctx context.Context, keySet string,
		value string, ttl time.Duration, gotKeyGet string,
	) (bool, []byte, error) {
		assert.Equal(t, keyLock, keySet)
		assert.Equal(t, lockTTL, ttl)
		assert.Equal(t, keyGet, gotKeyGet)
		if len(redisCache.LockGetCalls()) == 2 {
			return false, []byte(foobar), nil
		}
		return false, nil, nil
	}

	redisCache.ListenFunc = func(ctx context.Context, key string,
		ready ...func() error,
	) (string, error) {
		for _, fn := range ready {
			if err := fn(); err != nil {
				return "", err
			}
		}
		return "", nil
	}

	l := cache.lock(keyLock, keyGet)
	ok, b, err := l.lockGet(ctx, lockTTL, NewWaitLockIter(time.Second))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, []byte(foobar), b)
	assert.Len(t, redisCache.LockGetCalls(), 2)
	assert.Len(t, redisCache.ListenCalls(), 1)
}

func TestLock_WithLock_canceledBeforeRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sig := make(chan struct{})
	ttl := time.Second

	redisCache := &MoqRedisCache{
		ExpireFunc: func(ctx context.Context, key string, ttl time.Duration) (bool,
			error,
		) {
			sig <- struct{}{}
			cancel()
			return true, nil
		},
	}
	cache := New().WithRedisCache(redisCache)

	l := cache.lock(cache.ResolveKeyLock(testKey), cache.ResolveKey(testKey))
	err := l.WithLock(ctx, ttl, 10*time.Millisecond, func() error {
		<-sig
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestCache_OnceLock_withErrRedisCache(t *testing.T) {
	const foobar = "foobar"
	ctx := t.Context()
	testErr := errors.New("test error")

	tests := []struct {
		name      string
		configure func(t *testing.T, c *Cache) func()
		itemDo    func()
		failed    bool
	}{
		{
			name: "with Err set",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{}
				c.WithRedisCache(redisCache).redisCacheError(testErr)
				return nil
			},
			failed: true,
		},
		{
			name: "redisLockGet ErrRedisCache 1",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					LockGetFunc: func(ctx context.Context, keySet, value string,
						ttl time.Duration, keyGet string,
					) (bool, []byte, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), keySet)
						assert.Equal(t, lockTTL, ttl)
						assert.Equal(t, testKey, keyGet)
						return false, nil, testErr
					},
				}
				c.WithRedisCache(redisCache)
				return func() { assert.Len(t, redisCache.LockGetCalls(), 1) }
			},
			failed: true,
		},
		{
			name: "redisLockGet ErrRedisCache 2",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					LockGetFunc: func(ctx context.Context, keySet, value string,
						ttl time.Duration, keyGet string,
					) (bool, []byte, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), keySet)
						assert.Equal(t, lockTTL, ttl)
						assert.Equal(t, testKey, keyGet)
						return true, []byte(foobar), nil
					},
					UnlockFunc: func(ctx context.Context, key, value string) (bool, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), key)
						return false, testErr
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Len(t, redisCache.LockGetCalls(), 1)
					assert.Len(t, redisCache.UnlockCalls(), 1)
				}
			},
			failed: true,
		},
		{
			name: "waitUnlock ErrRedisCache 1",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					LockGetFunc: func(ctx context.Context, keySet, value string,
						ttl time.Duration, keyGet string,
					) (bool, []byte, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), keySet)
						assert.Equal(t, lockTTL, ttl)
						assert.Equal(t, testKey, keyGet)
						return false, nil, nil
					},
					ListenFunc: func(ctx context.Context, key string,
						ready ...func() error,
					) (string, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), key)
						for _, fn := range ready {
							if err := fn(); err != nil {
								return "", err
							}
						}
						return "", testErr
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Len(t, redisCache.LockGetCalls(), 1)
					assert.Len(t, redisCache.ListenCalls(), 1)
				}
			},
			failed: true,
		},
		{
			name: "waitUnlock ErrRedisCache 2",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					LockGetFunc: func(ctx context.Context, keySet, value string,
						ttl time.Duration, keyGet string,
					) (bool, []byte, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), keySet)
						assert.Equal(t, lockTTL, ttl)
						assert.Equal(t, testKey, keyGet)
						return false, nil, nil
					},
					ListenFunc: func(ctx context.Context, key string,
						ready ...func() error,
					) (string, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), key)
						for _, fn := range ready {
							if err := fn(); err != nil {
								return "", err
							}
						}
						return "fake", nil
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Len(t, redisCache.LockGetCalls(), 2)
					assert.Len(t, redisCache.ListenCalls(), 1)
				}
			},
			failed: true,
		},
		{
			name: "redisLockGet notFound",
			configure: func(t *testing.T, c *Cache) func() {
				localCache := &MoqLocalCache{
					GetFunc: func(key string) []byte {
						assert.Equal(t, testKey, key)
						return nil
					},
					SetFunc: func(key string, data []byte) {
						assert.Equal(t, testKey, key)
						assert.Equal(t, []byte(foobar), data)
					},
				}
				c.WithLocalCache(localCache)

				redisCache := &MoqRedisCache{
					LockGetFunc: func(ctx context.Context, keySet, value string,
						ttl time.Duration, keyGet string,
					) (bool, []byte, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), keySet)
						assert.Equal(t, lockTTL, ttl)
						assert.Equal(t, testKey, keyGet)
						return true, []byte(foobar), nil
					},
					UnlockFunc: func(ctx context.Context, key, value string) (bool, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), key)
						return false, nil
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Len(t, localCache.GetCalls(), 1)
					assert.Len(t, redisCache.LockGetCalls(), 1)
					assert.Len(t, localCache.SetCalls(), 1)
					assert.Len(t, redisCache.UnlockCalls(), 1)
				}
			},
		},
		{
			name: "WithLock ErrRedisCache 1",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					LockGetFunc: func(ctx context.Context, keySet, value string,
						ttl time.Duration, keyGet string,
					) (bool, []byte, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), keySet)
						assert.Equal(t, lockTTL, ttl)
						assert.Equal(t, testKey, keyGet)
						return true, nil, nil
					},
					SetFunc: func(ctx context.Context, maxItems int,
						items iter.Seq[redis.Item],
					) error {
						assert.Equal(t, 1, maxItems)
						return testErr
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Len(t, redisCache.LockGetCalls(), 1)
					assert.Len(t, redisCache.SetCalls(), 1)
				}
			},
			failed: true,
		},
		{
			name: "WithLock ErrRedisCache 2",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					LockGetFunc: func(ctx context.Context, keySet, value string,
						ttl time.Duration, keyGet string,
					) (bool, []byte, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), keySet)
						assert.Equal(t, lockTTL, ttl)
						assert.Equal(t, testKey, keyGet)
						return true, nil, nil
					},
					ExpireFunc: func(ctx context.Context, key string, ttl time.Duration,
					) (bool, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), key)
						assert.Equal(t, lockTTL, ttl)
						return false, testErr
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Len(t, redisCache.LockGetCalls(), 1)
					assert.Len(t, redisCache.ExpireCalls(), 1)
				}
			},
			itemDo: func() { time.Sleep(10 * time.Millisecond) },
			failed: true,
		},
		{
			name: "WithLock notFound 1",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					LockGetFunc: func(ctx context.Context, keySet, value string,
						ttl time.Duration, keyGet string,
					) (bool, []byte, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), keySet)
						assert.Equal(t, lockTTL, ttl)
						assert.Equal(t, testKey, keyGet)
						return true, nil, nil
					},
					SetFunc: func(ctx context.Context, maxItems int,
						items iter.Seq[redis.Item],
					) error {
						assert.Equal(t, 1, maxItems)
						return nil
					},
					UnlockFunc: func(ctx context.Context, key, value string) (bool, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), key)
						return false, nil
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Len(t, redisCache.LockGetCalls(), 1)
					assert.Len(t, redisCache.SetCalls(), 1)
					assert.Len(t, redisCache.UnlockCalls(), 1)
				}
			},
		},
		{
			name: "WithLock notFound 2",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					LockGetFunc: func(ctx context.Context, keySet, value string,
						ttl time.Duration, keyGet string,
					) (bool, []byte, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), keySet)
						assert.Equal(t, lockTTL, ttl)
						assert.Equal(t, testKey, keyGet)
						return true, nil, nil
					},
					ExpireFunc: func(ctx context.Context, key string, ttl time.Duration,
					) (bool, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), key)
						assert.Equal(t, lockTTL, ttl)
						return false, nil
					},
					SetFunc: func(ctx context.Context, maxItems int,
						items iter.Seq[redis.Item],
					) error {
						assert.Equal(t, 1, maxItems)
						return nil
					},
					UnlockFunc: func(ctx context.Context, key, value string) (bool, error) {
						assert.Equal(t, c.ResolveKeyLock(testKey), key)
						return false, nil
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Len(t, redisCache.LockGetCalls(), 1)
					assert.Len(t, redisCache.ExpireCalls(), 1)
					assert.Len(t, redisCache.SetCalls(), 1)
					assert.Len(t, redisCache.UnlockCalls(), 1)
				}
			},
			itemDo: func() { time.Sleep(10 * time.Millisecond) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New()
			assertFunc := tt.configure(t, c)
			c = c.New(WithLock(c.cfgLock.TTL, time.Millisecond, c.cfgLock.Iter))

			var got string
			err := c.OnceLock(ctx, Item{
				Key:   testKey,
				Value: &got,
				Do: func(ctx context.Context) (any, error) {
					if tt.itemDo != nil {
						tt.itemDo()
					}
					return foobar, nil
				},
			})
			require.NoError(t, err)

			assert.Equal(t, tt.failed, c.Failed())
			assert.Equal(t, foobar, got)

			if assertFunc != nil {
				assertFunc()
			}
		})
	}
}
