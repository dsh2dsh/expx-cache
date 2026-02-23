package model

import (
	"time"
)

type RedisItem struct {
	Key   string
	Value []byte
	TTL   time.Duration
}

func NewRedisItem(key string, value []byte, ttl time.Duration) RedisItem {
	return RedisItem{Key: key, Value: value, TTL: ttl}
}
