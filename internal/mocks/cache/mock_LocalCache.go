// Code generated by mockery. DO NOT EDIT.

package cache

import mock "github.com/stretchr/testify/mock"

// MockLocalCache is an autogenerated mock type for the LocalCache type
type MockLocalCache struct {
	mock.Mock
}

type MockLocalCache_Expecter struct {
	mock *mock.Mock
}

func (_m *MockLocalCache) EXPECT() *MockLocalCache_Expecter {
	return &MockLocalCache_Expecter{mock: &_m.Mock}
}

// Del provides a mock function with given fields: key
func (_m *MockLocalCache) Del(key string) {
	_m.Called(key)
}

// MockLocalCache_Del_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Del'
type MockLocalCache_Del_Call struct {
	*mock.Call
}

// Del is a helper method to define mock.On call
//   - key string
func (_e *MockLocalCache_Expecter) Del(key interface{}) *MockLocalCache_Del_Call {
	return &MockLocalCache_Del_Call{Call: _e.mock.On("Del", key)}
}

func (_c *MockLocalCache_Del_Call) Run(run func(key string)) *MockLocalCache_Del_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MockLocalCache_Del_Call) Return() *MockLocalCache_Del_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockLocalCache_Del_Call) RunAndReturn(run func(string)) *MockLocalCache_Del_Call {
	_c.Call.Return(run)
	return _c
}

// Get provides a mock function with given fields: key
func (_m *MockLocalCache) Get(key string) []byte {
	ret := _m.Called(key)

	if len(ret) == 0 {
		panic("no return value specified for Get")
	}

	var r0 []byte
	if rf, ok := ret.Get(0).(func(string) []byte); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	return r0
}

// MockLocalCache_Get_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Get'
type MockLocalCache_Get_Call struct {
	*mock.Call
}

// Get is a helper method to define mock.On call
//   - key string
func (_e *MockLocalCache_Expecter) Get(key interface{}) *MockLocalCache_Get_Call {
	return &MockLocalCache_Get_Call{Call: _e.mock.On("Get", key)}
}

func (_c *MockLocalCache_Get_Call) Run(run func(key string)) *MockLocalCache_Get_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MockLocalCache_Get_Call) Return(_a0 []byte) *MockLocalCache_Get_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockLocalCache_Get_Call) RunAndReturn(run func(string) []byte) *MockLocalCache_Get_Call {
	_c.Call.Return(run)
	return _c
}

// Set provides a mock function with given fields: key, data
func (_m *MockLocalCache) Set(key string, data []byte) {
	_m.Called(key, data)
}

// MockLocalCache_Set_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Set'
type MockLocalCache_Set_Call struct {
	*mock.Call
}

// Set is a helper method to define mock.On call
//   - key string
//   - data []byte
func (_e *MockLocalCache_Expecter) Set(key interface{}, data interface{}) *MockLocalCache_Set_Call {
	return &MockLocalCache_Set_Call{Call: _e.mock.On("Set", key, data)}
}

func (_c *MockLocalCache_Set_Call) Run(run func(key string, data []byte)) *MockLocalCache_Set_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].([]byte))
	})
	return _c
}

func (_c *MockLocalCache_Set_Call) Return() *MockLocalCache_Set_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockLocalCache_Set_Call) RunAndReturn(run func(string, []byte)) *MockLocalCache_Set_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockLocalCache creates a new instance of MockLocalCache. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockLocalCache(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockLocalCache {
	mock := &MockLocalCache{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
