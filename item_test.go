package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestItem_ttlTooSmall(t *testing.T) {
	item := &Item{TTL: 500 * time.Millisecond}
	assert.Equal(t, time.Second, item.ttl())
}
