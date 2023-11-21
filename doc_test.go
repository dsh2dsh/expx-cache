package cache_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	cache "github.com/dsh2dsh/expx-cache"
)

type Object struct {
	Str string
	Num int
}

func MustNewRedisCmdable() redis.Cmdable {
	rdb, err := cache.NewRedisClient()
	if err != nil {
		panic(err)
	} else if rdb == nil {
		return nil
	}
	return rdb
}

func Example_basicUsage() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithTinyLFU(1000, time.Minute).WithRedis(rdb)

	ctx := context.Background()
	key := "mykey"
	obj := &Object{
		Str: "mystring",
		Num: 42,
	}

	if err := mycache.Set(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: obj,
		TTL:   time.Hour,
	}); err != nil {
		log.Fatal(err)
	}

	var wanted Object
	if hit, err := mycache.Get(ctx, key, &wanted); err != nil {
		log.Fatal(err)
	} else if hit {
		fmt.Println(wanted)
	} else {
		fmt.Println("not found")
	}
}

func Example_advancedUsage() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithTinyLFU(1000, time.Minute).WithRedis(rdb)

	obj := new(Object)
	if err := mycache.Once(&cache.Item{
		Key:   "mykey",
		Value: obj, // destination
		Do: func(*cache.Item) (any, error) {
			return &Object{
				Str: "mystring",
				Num: 42,
			}, nil
		},
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println(obj)
}
