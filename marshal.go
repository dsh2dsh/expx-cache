package cache

import (
	"fmt"

	"github.com/klauspost/compress/s2"
	"github.com/vmihailenco/msgpack/v5"
)

func (self *Cache) Marshal(value any) ([]byte, error) {
	return self.marshal(value)
}

func (self *Cache) Unmarshal(b []byte, value any) error {
	return self.unmarshal(b, value)
}

func (self *Cache) WithMarshal(fn MarshalFunc) *Cache {
	self.marshal = fn
	return self
}

func (self *Cache) WithUnmarshal(fn UnmarshalFunc) *Cache {
	self.unmarshal = fn
	return self
}

// --------------------------------------------------

func marshal(value any) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal: msgpack error: %w", err)
	}

	return compress(b), nil
}

const (
	noCompression = 0x0
	s2Compression = 0x1
)

func unmarshal(b []byte, value any) error {
	if len(b) == 0 {
		return nil
	}

	switch value := value.(type) {
	case nil:
		return nil
	case *[]byte:
		clone := make([]byte, len(b))
		copy(clone, b)
		*value = clone
		return nil
	case *string:
		*value = string(b)
		return nil
	}

	switch c := b[len(b)-1]; c {
	case noCompression:
		b = b[:len(b)-1]
	case s2Compression:
		b = b[:len(b)-1]
		if decoded, err := s2.Decode(nil, b); err != nil {
			return fmt.Errorf("unmarshal: decompress error: %w", err)
		} else {
			b = decoded
		}
	default:
		return fmt.Errorf("unmarshal: unknown compression method: %x", c)
	}

	if err := msgpack.Unmarshal(b, value); err != nil {
		return fmt.Errorf("unmarshal msgpack error: %w", err)
	}

	return nil
}

func compress(data []byte) []byte {
	const compressionThreshold = 64
	if len(data) < compressionThreshold {
		b := make([]byte, len(data)+1)
		copy(b, data)
		b = b[:len(data)]               // make a sub-slice for safe append
		return append(b, noCompression) //nolint:makezero // b is a sub-slice
	}

	b := make([]byte, s2.MaxEncodedLen(len(data))+1)
	b = s2.Encode(b, data)
	return append(b, s2Compression) //nolint:makezero // b is a sub-slice
}
