package utils

import (
    "sync";
)

type Set interface {
    Len() int
    Add(interface{})
    Remove(interface{})
    Contains(interface{}) bool
    ToIterator() <-chan interface{}
    ToSlice() []interface{}
}

type baseSet struct {
    elements map[interface{}]struct{}
    mutex *sync.Mutex
}

func NewSet() Set {
    return &baseSet{
        elements: make(map[interface{}]struct{}),
        mutex: &sync.Mutex{},
    }
}

func (s *baseSet) Len() int {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return len(s.elements)
}

func (s *baseSet) Add(element interface{}) {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    s.elements[element] = struct{}{}
}

func (s *baseSet) Remove(element interface{}) {
    defer s.mutex.Unlock()
    s.mutex.Lock()
    
    delete(s.elements, element)
}

func(s *baseSet) Contains(element interface{}) bool {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    _, exists := s.elements[element]
    return exists
}

func (s *baseSet) ToIterator() <-chan interface{} {
    returnChan := make(chan interface{})
    go func() {
        defer s.mutex.Unlock()
        s.mutex.Lock()

        for element, _ := range s.elements {
            returnChan <- element
        }
        close(returnChan)
    }()
    return returnChan
}

func (s *baseSet) ToSlice() []interface{} {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    slice := make([]interface{}, 0, len(s.elements))
    for element, _ := range s.elements {
        slice = append(slice, element)
    }
    return slice
}
