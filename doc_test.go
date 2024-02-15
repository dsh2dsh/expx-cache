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

func MustNewRedisCmdable() *redis.Client {
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
	obj := Object{
		Str: "mystring",
		Num: 42,
	}

	item := cache.Item{Key: key, Value: &obj, TTL: time.Hour}
	if err := mycache.Set(ctx, item); err != nil {
		log.Fatal(err)
	}

	var wanted Object
	item.Value = &wanted
	if missed, err := mycache.Get(ctx, item); err != nil {
		log.Fatal(err)
	} else if len(missed) == 0 {
		fmt.Println(wanted)
	} else {
		fmt.Println("not found")
	}
}

func Example_advancedUsage() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithTinyLFU(1000, time.Minute).WithRedis(rdb)

	obj := new(Object)
	if err := mycache.Once(context.Background(), cache.Item{
		Key:   "mykey",
		Value: obj, // destination
		Do: func(ctx context.Context) (any, error) {
			return Object{
				Str: "mystring",
				Num: 42,
			}, nil
		},
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println(obj)
}
