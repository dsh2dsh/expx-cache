package cache

import (
	"context"
	"errors"
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

	ctx := context.Background()
	b, err := l.Get(ctx, lockTTL, NewWaitLockIter(time.Second))
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, b)
}

func TestLock_releaseNoValue(t *testing.T) {
	var l lock
	require.ErrorContains(t, l.release(context.Background()), "empty value")
}

func TestLock_lockGet_getAfterSubscribe(t *testing.T) {
	ctx := context.Background()

	redisCache := mocks.NewMockRedisCache(t)
	cache := New().WithRedisCache(redisCache)

	keyLock := cache.ResolveKeyLock(testKey)
	keyGet := cache.ResolveKey(testKey)

	redisCache.EXPECT().
		LockGet(mock.Anything, keyLock, mock.Anything, lockTTL, keyGet).
		Return(false, nil, nil).Once()

	redisCache.EXPECT().Listen(mock.Anything, keyLock, mock.Anything).
		RunAndReturn(
			func(ctx context.Context, key string, ready ...func() error,
			) (string, error) {
				for _, fn := range ready {
					if err := fn(); err != nil {
						return "", err
					}
				}
				return "", nil
			}).Once()

	const foobar = "foobar"
	redisCache.EXPECT().
		LockGet(mock.Anything, keyLock, mock.Anything, lockTTL, keyGet).
		Return(false, []byte(foobar), nil).Once()

	l := cache.lock(keyLock, keyGet)
	ok, b, err := l.lockGet(ctx, lockTTL, NewWaitLockIter(time.Second))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, []byte(foobar), b)
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
		failed bool
	}{
		{
			name: "with Err set",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				_ = cache.redisCacheError(testErr)
				return cache
			},
			failed: true,
		},
		{
			name: "redisLockGet ErrRedisCache 1",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything,
						lockTTL, testKey).
					Return(false, nil, testErr)
				return cache
			},
			failed: true,
		},
		{
			name: "redisLockGet ErrRedisCache 2",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything,
						lockTTL, testKey).
					Return(true, []byte(foobar), nil)
				redisCache.EXPECT().Unlock(ctx, cache.ResolveKeyLock(testKey),
					mock.Anything).
					Return(false, testErr)
				return cache
			},
			failed: true,
		},
		{
			name: "waitUnlock ErrRedisCache 1",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)

				redisCache.EXPECT().
					LockGet(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything,
						lockTTL, testKey).
					Return(false, nil, nil)

				redisCache.EXPECT().
					Listen(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything).
					RunAndReturn(
						func(ctx context.Context, key string, ready ...func() error,
						) (string, error) {
							for _, fn := range ready {
								if err := fn(); err != nil {
									return "", err
								}
							}
							return "", testErr
						}).Once()

				return cache
			},
			failed: true,
		},
		{
			name: "waitUnlock ErrRedisCache 2",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)

				redisCache.EXPECT().
					LockGet(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything,
						lockTTL, testKey).
					Return(false, nil, nil)

				redisCache.EXPECT().
					Listen(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything).
					RunAndReturn(
						func(ctx context.Context, key string, ready ...func() error,
						) (string, error) {
							for _, fn := range ready {
								if err := fn(); err != nil {
									return "", err
								}
							}
							return "fake", nil
						}).Once()

				return cache
			},
			failed: true,
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
					LockGet(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything,
						lockTTL, testKey).
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
					LockGet(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything,
						lockTTL, testKey).
					Return(true, nil, nil)
				redisCache.EXPECT().Set(ctx, 1, mock.Anything).Return(testErr)
				return cache
			},
			failed: true,
		},
		{
			name: "WithLock ErrRedisCache 2",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything,
						lockTTL, testKey).
					Return(true, nil, nil)
				redisCache.EXPECT().
					Expire(mock.Anything, cache.ResolveKeyLock(testKey), lockTTL).
					Return(false, testErr)
				return cache
			},
			itemDo: func() { time.Sleep(10 * time.Millisecond) },
			failed: true,
		},
		{
			name: "WithLock notFound 1",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().
					LockGet(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything,
						lockTTL, testKey).
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
					LockGet(mock.Anything, cache.ResolveKeyLock(testKey), mock.Anything,
						lockTTL, testKey).
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
			assert.Equal(t, tt.failed, cache.Failed())
			assert.Equal(t, foobar, got)
		})
	}
}
