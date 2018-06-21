package utils

import (
    "sync";
)

type Stack interface {
    Len() int
    Push(interface{})
    Pop() interface{}
}

// Thread safe stack implemented as linked list.
type stack struct {
    top *stackItem
    itemsCount int
    mutex *sync.Mutex
}

type stackItem struct {
    value interface{}
    next *stackItem
}

func NewStack() Stack {
    return &stack{
        mutex: &sync.Mutex{},
    }
}

func (s *stack) Len() int {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.itemsCount
}

func (s *stack) Push(value interface{}) {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    s.top = &stackItem{value, s.top}
    s.itemsCount++
}

func (s *stack) Pop() interface{} {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    if s.top == nil {
        panic("Empty stack")
    }

    var value interface{}
    value, s.top = s.top.value, s.top.next
    s.itemsCount--

    return value
}
