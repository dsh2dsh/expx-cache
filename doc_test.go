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
	rdb, err := cache.NewRedisClient(0)
	if err != nil {
		panic(err)
	}
	return rdb
}

func Example_basicUsage() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithNamespace("expx-cache-v0:").
		WithTinyLFU(1000, time.Hour).WithRedis(rdb)

	ctx := context.Background()
	key := "mykey1"
	obj := Object{Str: "mystring1", Num: 42}

	item := cache.Item{Key: key, Value: &obj}
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
	// Output: {mystring1 42}
}

func Example_advancedUsage() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithNamespace("expx-cache-v0:").
		WithTinyLFU(1000, time.Hour).WithRedis(rdb)

	var obj Object
	if err := mycache.Once(context.Background(), cache.Item{
		Key:   "mykey2",
		Value: &obj, // destination
		Do: func(ctx context.Context) (any, error) {
			return Object{Str: "mystring2", Num: 42}, nil
		},
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println(obj)
	// Output: {mystring2 42}
}

func ExampleNew() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New(
		cache.WithLock(10*time.Second, 3*time.Second, func() cache.WaitLockIter {
			return cache.NewWaitLockIter(time.Second, 10*time.Second)
		})).
		WithDefaultTTL(24*time.Hour).
		WithNamespace("expx-cache-v0:").
		WithTinyLFU(1000, time.Hour).WithRedis(rdb)

	fmt.Println(mycache.ResolveKey("mykey3"))
	// Output: expx-cache-v0:mykey3
}

func ExampleCache_Get() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithNamespace("expx-cache-v0:").
		WithTinyLFU(1000, time.Hour).WithRedis(rdb)

	var value1, value2 Object
	missed, err := mycache.Get(context.Background(),
		cache.Item{Key: "mykey1", Value: &value1},
		cache.Item{Key: "mykey2", Value: &value2},
		cache.Item{Key: "mykey3"},
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("missed:", len(missed), missed[0].Key)
	fmt.Println("value1:", value1)
	fmt.Println("value2:", value2)
	// Output:
	// missed: 1 mykey3
	// value1: {mystring1 42}
	// value2: {mystring2 42}
}

func ExampleCache_Set() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithNamespace("expx-cache-v0:").
		WithTinyLFU(1000, time.Hour).WithRedis(rdb)
	value1 := Object{Str: "mystring3", Num: 42}
	value2 := Object{Str: "mystring4", Num: 42}
	err := mycache.Set(context.Background(),
		cache.Item{Key: "mykey3", Value: &value1},
		cache.Item{Key: "mykey4", Value: &value2},
	)
	fmt.Println(err)
	// Output:
	// <nil>
}

func ExampleCache_GetSet() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithNamespace("expx-cache-v0:").
		WithTinyLFU(1000, time.Hour).WithRedis(rdb)

	var value1, value2 Object
	err := mycache.GetSet(context.Background(),
		cache.Item{
			Key:   "mykey1",
			Value: &value1,
			Do: func(ctx context.Context) (any, error) {
				return Object{Str: "mystring1", Num: 42}, nil
			},
		},
		cache.Item{
			Key:   "mykey5",
			Value: &value2,
			Do: func(ctx context.Context) (any, error) {
				return Object{Str: "mystring5", Num: 42}, nil
			},
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("value1:", value1)
	fmt.Println("value2:", value2)
	// Output:
	// value1: {mystring1 42}
	// value2: {mystring5 42}
}

func ExampleCache_OnceLock() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithNamespace("expx-cache-v0:").
		WithTinyLFU(1000, time.Hour).WithRedis(rdb)

	var obj Object
	if err := mycache.OnceLock(context.Background(), cache.Item{
		Key:   "mykey1",
		Value: &obj, // destination
		Do: func(ctx context.Context) (any, error) {
			return Object{Str: "mystring6", Num: 42}, nil
		},
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println(obj)
	// Output: {mystring1 42}
}

func ExampleCache_Exists() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithTinyLFU(1000, time.Hour).WithRedis(rdb)

	ok, err := mycache.Exists(context.Background(), "mykey1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ok)

	mycache.WithNamespace("expx-cache-v0:")
	ok, err = mycache.Exists(context.Background(), "mykey1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ok)

	// Output:
	// false
	// true
}

func ExampleCache_Delete() {
	rdb := MustNewRedisCmdable()
	mycache := cache.New().WithNamespace("expx-cache-v0:").
		WithTinyLFU(1000, time.Hour).WithRedis(rdb)
	if err := mycache.Delete(
		context.Background(), "mykey1", "mykey2", "mykey3", "mykey4", "mykey5",
	); err != nil {
		log.Fatal(err)
	}
	// Output:
}
