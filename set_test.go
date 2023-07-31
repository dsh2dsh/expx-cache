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
