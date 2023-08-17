package cache

import "context"

func (self *CacheTestSuite) TestSet_canBeUsedWithIncr() {
	if self.rdb == nil {
		self.T().Skip("requires Redis connection")
	}

	ctx := context.Background()
	value := "123"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: value,
	}))

	n := valueNoError[int64](self.T())(self.rdb.Incr(ctx, testKey).Result())
	self.Equal(int64(124), n)
}

func (self *CacheTestSuite) TestSetNX() {
	if self.rdb == nil {
		self.T().Skip("requires Redis connection")
	}

	ctx := context.Background()
	value := "foobar"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value,
		SetNX:          true,
		SkipLocalCache: true,
	}))

	var got string
	self.Require().NoError(self.cache.GetSkippingLocalCache(ctx, testKey, &got))
	self.Equal(value, got)

	value2 := "barfoo"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value2,
		SetNX:          true,
		SkipLocalCache: true,
	}))

	self.Require().NoError(self.cache.GetSkippingLocalCache(ctx, testKey, &got))
	self.Equal(value, got)
}

func (self *CacheTestSuite) TestSetXX() {
	if self.rdb == nil {
		self.T().Skip("requires Redis connection")
	}

	ctx := context.Background()
	value := "foobar"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value,
		SetXX:          true,
		SkipLocalCache: true,
	}))

	self.ErrorIs(self.cache.GetSkippingLocalCache(ctx, testKey, nil), ErrCacheMiss)

	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value,
		SkipLocalCache: true,
	}))

	var got string
	self.Require().NoError(self.cache.GetSkippingLocalCache(ctx, testKey, &got))
	self.Equal(value, got)

	value = "barfoo"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value,
		SetXX:          true,
		SkipLocalCache: true,
	}))

	self.Require().NoError(self.cache.GetSkippingLocalCache(ctx, testKey, &got))
	self.Equal(value, got)
}
