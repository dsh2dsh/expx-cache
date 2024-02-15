package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
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
				if self.cache.statsEnabled {
					self.stats.localHit()
				}
				return self.cache
			},
			assertCount: func(callCount uint32) {
				self.Equal(uint32(1), callCount)
			},
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		self.Run(tt.name, func() {
			self.TearDownTest()
			self.SetupTest()
			self.cache = self.cache.New(self.withLock())
			sig := make(chan struct{})
			var callCount uint32

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

func (self *CacheTestSuite) withLock() Option {
	return WithLock(self.cache.cfgLock.TTL, self.cache.cfgLock.Tick,
		func() LockWaitIter {
			tick := 60 * time.Millisecond
			return NewLockWaitIter(tick, 0, tick)
		})
}

func (self *CacheTestSuite) TestLock_Get_notExists() {
	self.needsRedis()

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey), testKey)
	ctx := context.Background()
	lockTTL := 10 * time.Second
	b, err := l.Get(ctx, lockTTL, NewLockWaitIter(time.Second, 0, time.Second))
	self.Require().NoError(err)
	self.Nil(b)

	var gotLockVal string
	item := Item{
		Key:            self.cache.ResolveKeyLock(testKey),
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

	const foobar = "foobar"
	ctx := context.Background()
	self.Require().NoError(self.cache.Set(ctx, Item{
		Key: testKey, Value: foobar, SkipLocalCache: true,
	}))

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey), testKey)
	lockTTL := 10 * time.Second
	b, err := l.Get(ctx, lockTTL, NewLockWaitIter(time.Second, 0, time.Second))
	self.Require().NoError(err)
	self.Equal([]byte(foobar), b)

	var gotLockVal string
	missed, err := self.cache.Get(ctx, Item{
		Key:            self.cache.ResolveKeyLock(testKey),
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
		return NewLockWaitIter(time.Second, 0, time.Second)
	}

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey), testKey)
	ctx := context.Background()
	lockTTL := 10 * time.Second
	b, err := l.Get(ctx, lockTTL, makeFreqIter())
	self.Require().NoError(err)
	self.Nil(b)

	self.Require().NoError(self.cache.Set(ctx, Item{
		Key: testKey, Value: foobar, SkipLocalCache: true,
	}))

	l2 := self.cache.lock(self.cache.ResolveKeyLock(testKey), testKey)
	b, err = l2.Get(ctx, lockTTL, makeFreqIter())
	self.Require().NoError(err)
	self.Equal([]byte(foobar), b)

	var gotLockVal string
	missed, err := self.cache.Get(ctx, Item{
		Key:            self.cache.ResolveKeyLock(testKey),
		Value:          &gotLockVal,
		SkipLocalCache: true,
	})
	self.Require().NoError(err)
	self.Empty(missed, "lock doesn't exist")
}

func (self *CacheTestSuite) TestLock_Get_waitLockDeadline() {
	self.needsRedis()

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey), testKey)
	ctx := context.Background()
	b, err := l.Get(ctx, lockTTL, NewLockWaitIter(time.Second, 0, time.Second))
	self.Require().NoError(err)
	self.Nil(b)

	ctx2, cancel := context.WithDeadline(ctx, time.Now().Add(100*time.Millisecond))
	defer cancel()
	l2 := self.cache.lock(self.cache.ResolveKeyLock(testKey), testKey)
	_, err = l2.Get(ctx2, lockTTL, NewLockWaitIter(
		10*time.Millisecond, 0, 10*time.Millisecond))
	self.Require().ErrorIs(err, context.DeadlineExceeded)

	var gotLockVal string
	missed, err := self.cache.Get(ctx, Item{
		Key:            self.cache.ResolveKeyLock(testKey),
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

	const foobar = "foobar"
	self.cache.requestId = foobar
	l := self.cache.lock(self.cache.ResolveKeyLock(testKey), testKey)
	ctx := context.Background()
	lockTTL := 10 * time.Second
	b, err := l.Get(ctx, lockTTL, NewLockWaitIter(time.Second, 0, time.Second))
	self.Require().NoError(err)
	self.Nil(b)

	var gotLockVal string
	missed, err := self.cache.Get(ctx, Item{
		Key:            self.cache.ResolveKeyLock(testKey),
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

	l := self.cache.lock(self.cache.ResolveKeyLock(testKey), testKey)
	ctx := context.Background()
	lockTTL := 10 * time.Second
	b, err := l.Get(ctx, lockTTL, NewLockWaitIter(time.Second, 0, time.Second))
	self.Require().NoError(err)
	self.Nil(b)

	var callCount int
	err = l.WithLock(ctx, lockTTL, 30*time.Millisecond, func() error {
		callCount++
		var s string
		missed, err := self.cache.Get(ctx, Item{
			Key:            self.cache.ResolveKeyLock(testKey),
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
		Key:            self.cache.ResolveKeyLock(testKey),
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

func TestDurationIter(t *testing.T) {
	tests := []struct {
		name       string
		N          int
		minTime    time.Duration
		keepMin    int
		maxTime    time.Duration
		beginsWith []string
	}{
		{
			N:          3,
			minTime:    time.Second,
			keepMin:    0,
			maxTime:    time.Second,
			beginsWith: []string{"1s", "1s", "1s"},
		},
		{
			N:       3,
			minTime: time.Second,
			keepMin: 0,
			maxTime: 10 * time.Second,
		},
		{
			N:          3,
			minTime:    time.Second,
			keepMin:    1,
			maxTime:    time.Second,
			beginsWith: []string{"1s", "1s", "1s"},
		},
		{
			N:          3,
			minTime:    time.Second,
			keepMin:    1,
			maxTime:    10 * time.Second,
			beginsWith: []string{"1s"},
		},
		{
			N:          5,
			minTime:    time.Second,
			keepMin:    2,
			maxTime:    10 * time.Second,
			beginsWith: []string{"1s", "1s"},
		},
		{
			N:          5,
			minTime:    time.Second,
			keepMin:    5,
			maxTime:    10 * time.Second,
			beginsWith: []string{"1s", "1s", "1s", "1s", "1s"},
		},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("%v %s,%v,%s", tt.N, tt.minTime, tt.keepMin, tt.maxTime)
		t.Run(name, func(t *testing.T) {
			iter := NewLockWaitIter(tt.minTime, tt.keepMin, tt.maxTime)
			gotTimes := make([]time.Duration, tt.N)
			strTimes := make([]string, tt.N)
			for i := 0; i < len(gotTimes); i++ {
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

	ctx := context.Background()
	b, err := l.Get(ctx, lockTTL, NewLockWaitIter(time.Second, 0, time.Second))
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, b)
}

func TestLock_releaseNoValue(t *testing.T) {
	var l lock
	require.ErrorContains(t, l.release(context.Background()), "empty value")
}

func TestLock_WithLock_canceledBeforeRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan struct{})
	ttl := time.Second

	redisCache := mocks.NewMockRedisCache(t)
	cache := New().WithRedisCache(redisCache)
	redisCache.EXPECT().Expire(mock.Anything, cache.ResolveKeyLock(testKey), ttl).
		RunAndReturn(
			func(ctx context.Context, key string, ttl time.Duration) (bool, error) {
				sig <- struct{}{}
				cancel()
				return true, nil
			})

	l := cache.lock(cache.ResolveKeyLock(testKey), cache.ResolveKey(testKey))
	err := l.WithLock(ctx, ttl, 10*time.Millisecond, func() error {
		<-sig
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestCache_OnceLock_withErrRedisCache(t *testing.T) {
	const foobar = "foobar"
	ctx := context.Background()
	testErr := errors.New("test error")

	tests := []struct {
		name   string
		cache  func() *Cache
		itemDo func()
		err    error
	}{
		{
			name: "with Err set",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				_ = cache.redisCacheError(testErr)
				return cache
			},
			err: ErrRedisCache,
		},
		{
			name: "redisLockGet ErrRedisCache 1",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(ctx, cache.ResolveKeyLock(testKey), mock.Anything, lockTTL,
						testKey).
					Return(false, nil, testErr)
				return cache
			},
			err: ErrRedisCache,
		},
		{
			name: "redisLockGet ErrRedisCache 2",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(ctx, cache.ResolveKeyLock(testKey), mock.Anything, lockTTL,
						testKey).
					Return(true, []byte(foobar), nil)
				redisCache.EXPECT().Unlock(ctx, cache.ResolveKeyLock(testKey),
					mock.Anything).
					Return(false, testErr)
				return cache
			},
			err: ErrRedisCache,
		},
		{
			name: "redisLockGet notFound",
			cache: func() *Cache {
				localCache := mocks.NewMockLocalCache(t)
				localCache.EXPECT().Get(testKey).Return(nil)
				localCache.EXPECT().Set(testKey, []byte(foobar))
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(ctx, cache.ResolveKeyLock(testKey), mock.Anything, lockTTL,
						testKey).
					Return(true, []byte(foobar), nil)
				redisCache.EXPECT().Unlock(ctx, cache.ResolveKeyLock(testKey),
					mock.Anything).
					Return(false, nil)
				return cache
			},
		},
		{
			name: "WithLock ErrRedisCache 1",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(ctx, cache.ResolveKeyLock(testKey), mock.Anything, lockTTL,
						testKey).
					Return(true, nil, nil)
				redisCache.EXPECT().Set(ctx, 1, mock.Anything).Return(testErr)
				return cache
			},
			err: ErrRedisCache,
		},
		{
			name: "WithLock ErrRedisCache 2",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(ctx, cache.ResolveKeyLock(testKey), mock.Anything, lockTTL,
						testKey).
					Return(true, nil, nil)
				redisCache.EXPECT().
					Expire(mock.Anything, cache.ResolveKeyLock(testKey), lockTTL).
					Return(false, testErr)
				return cache
			},
			itemDo: func() { time.Sleep(10 * time.Millisecond) },
			err:    ErrRedisCache,
		},
		{
			name: "WithLock notFound 1",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(ctx, cache.ResolveKeyLock(testKey), mock.Anything, lockTTL,
						testKey).
					Return(true, nil, nil)
				redisCache.EXPECT().Set(ctx, 1, mock.Anything).Return(nil)
				redisCache.EXPECT().
					Unlock(ctx, cache.ResolveKeyLock(testKey), mock.Anything).
					Return(false, nil)
				return cache
			},
		},
		{
			name: "WithLock notFound 2",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(ctx, cache.ResolveKeyLock(testKey), mock.Anything, lockTTL,
						testKey).
					Return(true, nil, nil)
				redisCache.EXPECT().
					Expire(mock.Anything, cache.ResolveKeyLock(testKey), lockTTL).
					Return(false, nil)
				redisCache.EXPECT().Set(ctx, 1, mock.Anything).Return(nil)
				redisCache.EXPECT().
					Unlock(ctx, cache.ResolveKeyLock(testKey), mock.Anything).
					Return(false, nil)
				return cache
			},
			itemDo: func() { time.Sleep(10 * time.Millisecond) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.cache()
			cache = cache.New(WithLock(cache.cfgLock.TTL, time.Millisecond,
				cache.cfgLock.Iter))
			var got string
			err := cache.OnceLock(ctx, Item{
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
			require.ErrorIs(t, cache.Err(), tt.err)
			assert.Equal(t, foobar, got)
		})
	}
}
