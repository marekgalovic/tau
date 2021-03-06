package utils

import (
    "fmt";
    "sync";
    "strings";
)

type Set interface {
    Len() int
    Clear()
    Add(interface{})
    Remove(interface{})
    Contains(interface{}) bool
    Equal(Set) bool
    Intersection(Set) Set
    Difference(Set) Set
    Union(Set) Set
    ToIterator() <-chan interface{}
    ToSlice() []interface{}
    String() string
    Rand() interface{}
}

type baseSet map[interface{}]struct{}

type threadSafeSet struct {
    baseSet
    mutex *sync.Mutex
}

func NewSet(elements ...interface{}) Set {
    set := make(baseSet, len(elements))
    for _, element := range elements {
        set.Add(element)
    }
    return &set
}

func (s *baseSet) Len() int {
    return len(*s)
}

func (s *baseSet) Clear() {
    *s = make(baseSet)
}

func (s *baseSet) Add(element interface{}) {
    (*s)[element] = struct{}{}
}

func (s *baseSet) Remove(element interface{}) {
    delete(*s, element)
}

func(s *baseSet) Contains(element interface{}) bool {
    _, exists := (*s)[element]
    return exists
}

func (s *baseSet) Equal(other Set) bool {
    o := other.(*baseSet)

    if s.Len() != o.Len() {
        return false
    }

    for element, _ := range *s {
        if !o.Contains(element) {
            return false
        }
    }

    return true
}

func (s *baseSet) Intersection(other Set) Set {
    result := make(baseSet)

    for element, _ := range *s {
        if other.Contains(element) {
            result.Add(element)
        }
    }

    return &result
}

func (s *baseSet) Difference(other Set) Set {
    result := make(baseSet)

    for element, _ := range *s {
        if !other.Contains(element) {
            result.Add(element)
        }
    }

    return &result
}

func (s *baseSet) Union(other Set) Set {
    o := other.(*baseSet)
    result := make(baseSet)

    for element, _ := range *s {
        result.Add(element)
    }
    for element, _ := range *o {
        result.Add(element)
    }

    return &result
}

func (s *baseSet) ToIterator() <-chan interface{} {
    returnChan := make(chan interface{})
    go func() {
        for element, _ := range *s {
            returnChan <- element
        }
        close(returnChan)
    }()
    return returnChan
}

func (s *baseSet) ToSlice() []interface{} {
    slice := make([]interface{}, 0, s.Len())
    for element, _ := range *s {
        slice = append(slice, element)
    }
    return slice
}

func (s *baseSet) String() string {
    elementStrings := make([]string, s.Len())

    i := 0
    for element, _ := range *s {
        elementStrings[i] = fmt.Sprintf("%v", element)
        i++
    }

    return fmt.Sprintf("{%s}", strings.Join(elementStrings, ", "))
}

func (s *baseSet) Rand() interface{} {
    for element, _ := range *s {
        return element
    }

    return nil
}

func NewThreadSafeSet(elements ...interface{}) Set {
    set := &threadSafeSet {
        baseSet: make(baseSet, len(elements)),
        mutex: &sync.Mutex{},
    }
    for _, element := range elements {
        set.baseSet.Add(element)
    }
    return set
}

func (s *threadSafeSet) Len() int {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.baseSet.Len() 
}

func (s *threadSafeSet) Clear() {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    s.baseSet.Clear() 
}

func (s *threadSafeSet) Add(element interface{}) {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    s.baseSet.Add(element)
}

func (s *threadSafeSet) Remove(element interface{}) {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    s.baseSet.Remove(element) 
}

func (s *threadSafeSet) Contains(element interface{}) bool {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.baseSet.Contains(element)
}

func (s *threadSafeSet) Equal(other Set) bool {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.baseSet.Equal(other)
}

func (s *threadSafeSet) Difference(other Set) Set {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.baseSet.Difference(other)
}

func (s *threadSafeSet) Union(other Set) Set {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.baseSet.Union(other)
}

func (s *threadSafeSet) ToIterator() <-chan interface{} {
    returnChan := make(chan interface{})
    go func() {
        defer s.mutex.Unlock()
        s.mutex.Lock()

        for element, _ := range s.baseSet {
            returnChan <- element
        }
        close(returnChan)
    }()
    return returnChan
}

func (s *threadSafeSet) ToSlice() []interface{} {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.baseSet.ToSlice()
}

func (s *threadSafeSet) String() string {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.baseSet.String()
}

func (s *threadSafeSet) Rand() interface{} {
    defer s.mutex.Unlock()
    s.mutex.Lock()

    return s.baseSet.Rand()
}
