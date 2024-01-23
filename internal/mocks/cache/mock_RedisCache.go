// Code generated by mockery. DO NOT EDIT.

package cache

import (
	context "context"
	time "time"

	mock "github.com/stretchr/testify/mock"
)

// MockRedisCache is an autogenerated mock type for the RedisCache type
type MockRedisCache struct {
	mock.Mock
}

type MockRedisCache_Expecter struct {
	mock *mock.Mock
}

func (_m *MockRedisCache) EXPECT() *MockRedisCache_Expecter {
	return &MockRedisCache_Expecter{mock: &_m.Mock}
}

// Del provides a mock function with given fields: ctx, keys
func (_m *MockRedisCache) Del(ctx context.Context, keys []string) error {
	ret := _m.Called(ctx, keys)

	if len(ret) == 0 {
		panic("no return value specified for Del")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []string) error); ok {
		r0 = rf(ctx, keys)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockRedisCache_Del_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Del'
type MockRedisCache_Del_Call struct {
	*mock.Call
}

// Del is a helper method to define mock.On call
//   - ctx context.Context
//   - keys []string
func (_e *MockRedisCache_Expecter) Del(ctx interface{}, keys interface{}) *MockRedisCache_Del_Call {
	return &MockRedisCache_Del_Call{Call: _e.mock.On("Del", ctx, keys)}
}

func (_c *MockRedisCache_Del_Call) Run(run func(ctx context.Context, keys []string)) *MockRedisCache_Del_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].([]string))
	})
	return _c
}

func (_c *MockRedisCache_Del_Call) Return(_a0 error) *MockRedisCache_Del_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRedisCache_Del_Call) RunAndReturn(run func(context.Context, []string) error) *MockRedisCache_Del_Call {
	_c.Call.Return(run)
	return _c
}

// Get provides a mock function with given fields: ctx, maxItems, keyIter
func (_m *MockRedisCache) Get(ctx context.Context, maxItems int, keyIter func(int) string) (func() ([]byte, bool), error) {
	ret := _m.Called(ctx, maxItems, keyIter)

	if len(ret) == 0 {
		panic("no return value specified for Get")
	}

	var r0 func() ([]byte, bool)
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int, func(int) string) (func() ([]byte, bool), error)); ok {
		return rf(ctx, maxItems, keyIter)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int, func(int) string) func() ([]byte, bool)); ok {
		r0 = rf(ctx, maxItems, keyIter)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(func() ([]byte, bool))
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int, func(int) string) error); ok {
		r1 = rf(ctx, maxItems, keyIter)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockRedisCache_Get_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Get'
type MockRedisCache_Get_Call struct {
	*mock.Call
}

// Get is a helper method to define mock.On call
//   - ctx context.Context
//   - maxItems int
//   - keyIter func(int) string
func (_e *MockRedisCache_Expecter) Get(ctx interface{}, maxItems interface{}, keyIter interface{}) *MockRedisCache_Get_Call {
	return &MockRedisCache_Get_Call{Call: _e.mock.On("Get", ctx, maxItems, keyIter)}
}

func (_c *MockRedisCache_Get_Call) Run(run func(ctx context.Context, maxItems int, keyIter func(int) string)) *MockRedisCache_Get_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int), args[2].(func(int) string))
	})
	return _c
}

func (_c *MockRedisCache_Get_Call) Return(_a0 func() ([]byte, bool), _a1 error) *MockRedisCache_Get_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockRedisCache_Get_Call) RunAndReturn(run func(context.Context, int, func(int) string) (func() ([]byte, bool), error)) *MockRedisCache_Get_Call {
	_c.Call.Return(run)
	return _c
}

// Set provides a mock function with given fields: ctx, maxItems, iter
func (_m *MockRedisCache) Set(ctx context.Context, maxItems int, iter func(int) (string, []byte, time.Duration)) error {
	ret := _m.Called(ctx, maxItems, iter)

	if len(ret) == 0 {
		panic("no return value specified for Set")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, int, func(int) (string, []byte, time.Duration)) error); ok {
		r0 = rf(ctx, maxItems, iter)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockRedisCache_Set_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Set'
type MockRedisCache_Set_Call struct {
	*mock.Call
}

// Set is a helper method to define mock.On call
//   - ctx context.Context
//   - maxItems int
//   - iter func(int)(string , []byte , time.Duration)
func (_e *MockRedisCache_Expecter) Set(ctx interface{}, maxItems interface{}, iter interface{}) *MockRedisCache_Set_Call {
	return &MockRedisCache_Set_Call{Call: _e.mock.On("Set", ctx, maxItems, iter)}
}

func (_c *MockRedisCache_Set_Call) Run(run func(ctx context.Context, maxItems int, iter func(int) (string, []byte, time.Duration))) *MockRedisCache_Set_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int), args[2].(func(int) (string, []byte, time.Duration)))
	})
	return _c
}

func (_c *MockRedisCache_Set_Call) Return(_a0 error) *MockRedisCache_Set_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockRedisCache_Set_Call) RunAndReturn(run func(context.Context, int, func(int) (string, []byte, time.Duration)) error) *MockRedisCache_Set_Call {
	_c.Call.Return(run)
	return _c
}

// SetNxGet provides a mock function with given fields: ctx, keySet, value, ttl, keyGet
func (_m *MockRedisCache) SetNxGet(ctx context.Context, keySet string, value string, ttl time.Duration, keyGet string) (bool, []byte, error) {
	ret := _m.Called(ctx, keySet, value, ttl, keyGet)

	if len(ret) == 0 {
		panic("no return value specified for SetNxGet")
	}

	var r0 bool
	var r1 []byte
	var r2 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, time.Duration, string) (bool, []byte, error)); ok {
		return rf(ctx, keySet, value, ttl, keyGet)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string, time.Duration, string) bool); ok {
		r0 = rf(ctx, keySet, value, ttl, keyGet)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string, time.Duration, string) []byte); ok {
		r1 = rf(ctx, keySet, value, ttl, keyGet)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]byte)
		}
	}

	if rf, ok := ret.Get(2).(func(context.Context, string, string, time.Duration, string) error); ok {
		r2 = rf(ctx, keySet, value, ttl, keyGet)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// MockRedisCache_SetNxGet_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetNxGet'
type MockRedisCache_SetNxGet_Call struct {
	*mock.Call
}

// SetNxGet is a helper method to define mock.On call
//   - ctx context.Context
//   - keySet string
//   - value string
//   - ttl time.Duration
//   - keyGet string
func (_e *MockRedisCache_Expecter) SetNxGet(ctx interface{}, keySet interface{}, value interface{}, ttl interface{}, keyGet interface{}) *MockRedisCache_SetNxGet_Call {
	return &MockRedisCache_SetNxGet_Call{Call: _e.mock.On("SetNxGet", ctx, keySet, value, ttl, keyGet)}
}

func (_c *MockRedisCache_SetNxGet_Call) Run(run func(ctx context.Context, keySet string, value string, ttl time.Duration, keyGet string)) *MockRedisCache_SetNxGet_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(time.Duration), args[4].(string))
	})
	return _c
}

func (_c *MockRedisCache_SetNxGet_Call) Return(ok bool, b []byte, err error) *MockRedisCache_SetNxGet_Call {
	_c.Call.Return(ok, b, err)
	return _c
}

func (_c *MockRedisCache_SetNxGet_Call) RunAndReturn(run func(context.Context, string, string, time.Duration, string) (bool, []byte, error)) *MockRedisCache_SetNxGet_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockRedisCache creates a new instance of MockRedisCache. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockRedisCache(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockRedisCache {
	mock := &MockRedisCache{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
