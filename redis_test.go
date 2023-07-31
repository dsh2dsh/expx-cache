package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRefreshRedis(t *testing.T) {
	const ttl = time.Minute

	rr := NewRefreshRedis(nil, ttl)
	require.IsType(t, new(RefreshRedis), rr)
	assert.Nil(t, rr.rdb)
	assert.Equal(t, rr.refreshTTL, ttl)
	assert.Implements(t, (*RedisClient)(nil), new(RefreshRedis))
}

func TestNewStdRedis(t *testing.T) {
	sr := NewStdRedis(nil)
	require.IsType(t, new(StdRedis), sr)
	assert.Nil(t, sr.rdb)
	assert.Implements(t, (*RedisClient)(nil), new(StdRedis))
}
