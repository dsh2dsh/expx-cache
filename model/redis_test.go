package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewRedisItem(t *testing.T) {
	item := RedisItem{Key: "key1", Value: []byte("value1"), TTL: time.Hour}
	assert.Equal(t, item, NewRedisItem(item.Key, item.Value, item.TTL))
}
