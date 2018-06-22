package utils

import (
    "sync";
)

type Stack interface {
    Len() int
    Push(interface{})
    Pop() interface{}
}

type stack struct {
    top *stackItem
    itemsCount int
    mutex *sync.Mutex
}

type threadSafeStack struct {
    stack
    mutex *sync.Mutex
}

type stackItem struct {
    value interface{}
    next *stackItem
}

func NewStack() Stack {
    return &stack{}
}

func (s *stack) Len() int {
    return s.itemsCount
}

func (s *stack) Push(value interface{}) {
    s.top = &stackItem{value, s.top}
    s.itemsCount++
}

func (s *stack) Pop() interface{} {
    if s.top == nil {
        panic("Empty stack")
    }

    var value interface{}
    value, s.top = s.top.value, s.top.next
    s.itemsCount--

    return value
}

func NewThreadSafeStack() Stack {
    return &threadSafeStack {
        mutex: &sync.Mutex{},
    }
}

func (s *threadSafeStack) Len() int {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.stack.Len()
}

func (s *threadSafeStack) Push(value interface{}) {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    s.stack.Push(value)
}

func (s *threadSafeStack) Pop() interface{} {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.stack.Pop()
}
