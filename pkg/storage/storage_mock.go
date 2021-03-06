// Code generated by MockGen. DO NOT EDIT.
// Source: storage/storage.go

// Package storage is a generated GoMock package.
package storage

import (
	gomock "github.com/golang/mock/gomock"
	io "io"
	reflect "reflect"
)

// MockStorage is a mock of Storage interface
type MockStorage struct {
	ctrl     *gomock.Controller
	recorder *MockStorageMockRecorder
}

// MockStorageMockRecorder is the mock recorder for MockStorage
type MockStorageMockRecorder struct {
	mock *MockStorage
}

// NewMockStorage creates a new mock instance
func NewMockStorage(ctrl *gomock.Controller) *MockStorage {
	mock := &MockStorage{ctrl: ctrl}
	mock.recorder = &MockStorageMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockStorage) EXPECT() *MockStorageMockRecorder {
	return m.recorder
}

// Exists mocks base method
func (m *MockStorage) Exists(arg0 string) (bool, error) {
	ret := m.ctrl.Call(m, "Exists", arg0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exists indicates an expected call of Exists
func (mr *MockStorageMockRecorder) Exists(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exists", reflect.TypeOf((*MockStorage)(nil).Exists), arg0)
}

// ListFiles mocks base method
func (m *MockStorage) ListFiles(arg0 string) ([]string, error) {
	ret := m.ctrl.Call(m, "ListFiles", arg0)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListFiles indicates an expected call of ListFiles
func (mr *MockStorageMockRecorder) ListFiles(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListFiles", reflect.TypeOf((*MockStorage)(nil).ListFiles), arg0)
}

// Reader mocks base method
func (m *MockStorage) Reader(arg0 string) (io.ReadCloser, error) {
	ret := m.ctrl.Call(m, "Reader", arg0)
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Reader indicates an expected call of Reader
func (mr *MockStorageMockRecorder) Reader(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reader", reflect.TypeOf((*MockStorage)(nil).Reader), arg0)
}

// Writer mocks base method
func (m *MockStorage) Writer(arg0 string) (io.WriteCloser, error) {
	ret := m.ctrl.Call(m, "Writer", arg0)
	ret0, _ := ret[0].(io.WriteCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Writer indicates an expected call of Writer
func (mr *MockStorageMockRecorder) Writer(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Writer", reflect.TypeOf((*MockStorage)(nil).Writer), arg0)
}
