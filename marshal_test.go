package cache

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCache_WithMarshal(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	var called bool
	cache.WithMarshal(func(v any) ([]byte, error) {
		called = true
		return marshal(v)
	})

	ctx := context.Background()
	value := "abc"
	require.NoError(t, cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: value,
	}))

	assert.True(t, called, "custom marshall func wasn't called")
}

func TestCache_WithUnmarshal(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	ctx := context.Background()
	value := "abc"
	require.NoError(t, cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: value,
	}))

	var called bool
	cache.WithUnmarshal(func(b []byte, v any) error {
		called = true
		return unmarshal(b, v)
	})
	require.NoError(t, cache.Get(ctx, testKey, &value))
	assert.True(t, called, "custom unmarshall func wasn't called")
}

func TestCache_Marshal_nil(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)
	assert.Nil(t, valueNoError[[]byte](t)(cache.Marshal(nil)))
}

func TestCache_Marshal_compression(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	s := strings.Repeat("foobar", 100)
	b := valueNoError[[]byte](t)(cache.Marshal(&s))
	assert.NotNil(t, b)
	assert.Equal(t, s2Compression, int(b[len(b)-1]))
}

func TestCache_Marshal_noCompression(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	s := "foobar" //nolint:goconst // I don't want use const for this
	b := valueNoError[[]byte](t)(cache.Marshal(&s))
	assert.NotNil(t, b)
	assert.Equal(t, noCompression, int(b[len(b)-1]))
}

type msgpackErrItem struct {
	Foo string
}

func (self *msgpackErrItem) EncodeMsgpack(enc *msgpack.Encoder) error {
	return io.EOF
}

func TestCache_Marshal_msgpackErr(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	b, err := cache.Marshal(&msgpackErrItem{"bar"})
	assert.Error(t, err)
	assert.Nil(t, b)
}

func TestCache_Unmarshal_nil(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	assert.NoError(t, cache.Unmarshal([]byte{}, nil))
	assert.NoError(t, cache.Unmarshal([]byte("foobar"), nil))
}

func TestCache_Unmarshal_compression(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	type fooItem struct {
		Foo string
	}
	item := fooItem{
		Foo: strings.Repeat("foobar", 100),
	}

	b := valueNoError[[]byte](t)(cache.Marshal(&item))
	assert.Equal(t, s2Compression, int(b[len(b)-1]))

	gotItem := fooItem{}
	assert.NoError(t, cache.Unmarshal(b, &gotItem))
	assert.Equal(t, item, gotItem)

	assert.ErrorContains(t, cache.Unmarshal([]byte{0x0, 0xff}, &gotItem),
		"unknown compression method")

	assert.ErrorContains(t, cache.Unmarshal([]byte{0x1, s2Compression}, &gotItem),
		"unmarshal: decompress error")
}
