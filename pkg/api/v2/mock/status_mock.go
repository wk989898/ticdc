// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/api/v2/status.go

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	v2 "github.com/pingcap/ticdc/api/v2"
	v20 "github.com/pingcap/ticdc/pkg/api/v2"
)

// MockStatusGetter is a mock of StatusGetter interface.
type MockStatusGetter struct {
	ctrl     *gomock.Controller
	recorder *MockStatusGetterMockRecorder
}

// MockStatusGetterMockRecorder is the mock recorder for MockStatusGetter.
type MockStatusGetterMockRecorder struct {
	mock *MockStatusGetter
}

// NewMockStatusGetter creates a new mock instance.
func NewMockStatusGetter(ctrl *gomock.Controller) *MockStatusGetter {
	mock := &MockStatusGetter{ctrl: ctrl}
	mock.recorder = &MockStatusGetterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStatusGetter) EXPECT() *MockStatusGetterMockRecorder {
	return m.recorder
}

// Status mocks base method.
func (m *MockStatusGetter) Status() v20.StatusInterface {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Status")
	ret0, _ := ret[0].(v20.StatusInterface)
	return ret0
}

// Status indicates an expected call of Status.
func (mr *MockStatusGetterMockRecorder) Status() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Status", reflect.TypeOf((*MockStatusGetter)(nil).Status))
}

// MockStatusInterface is a mock of StatusInterface interface.
type MockStatusInterface struct {
	ctrl     *gomock.Controller
	recorder *MockStatusInterfaceMockRecorder
}

// MockStatusInterfaceMockRecorder is the mock recorder for MockStatusInterface.
type MockStatusInterfaceMockRecorder struct {
	mock *MockStatusInterface
}

// NewMockStatusInterface creates a new mock instance.
func NewMockStatusInterface(ctrl *gomock.Controller) *MockStatusInterface {
	mock := &MockStatusInterface{ctrl: ctrl}
	mock.recorder = &MockStatusInterfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStatusInterface) EXPECT() *MockStatusInterfaceMockRecorder {
	return m.recorder
}

// Get mocks base method.
func (m *MockStatusInterface) Get(ctx context.Context) (*v2.ServerStatus, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", ctx)
	ret0, _ := ret[0].(*v2.ServerStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockStatusInterfaceMockRecorder) Get(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockStatusInterface)(nil).Get), ctx)
}
