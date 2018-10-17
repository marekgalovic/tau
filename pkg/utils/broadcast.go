package utils

import (
    "sync";
)

type Broadcast interface {
    Close()
    Send(interface{})
    Listen(int) <-chan interface{}
}

type baseBroadcaster struct {
    listeners []chan interface{}
}

type threadSafeBraodcaster struct {
    baseBroadcaster *baseBroadcaster
    mutex *sync.Mutex
}

func NewBroadcast() Broadcast {
    return &baseBroadcaster{}
}

func NewThreadSafeBroadcast() Broadcast {
    return &threadSafeBraodcaster {
        baseBroadcaster: &baseBroadcaster{},
        mutex: &sync.Mutex{},
    }
}

// Base implementation
func (b *baseBroadcaster) Close() {
    for _, listener := range b.listeners {
        close(listener)
    }
}

func (b *baseBroadcaster) Send(value interface{}) {
    for _, listener := range b.listeners {
        select {
        case listener <- value:
            continue
        default:
        }
    }
}

func (b *baseBroadcaster) Listen(bufSize int) <-chan interface{} {
    listener := make(chan interface{}, bufSize)
    b.listeners = append(b.listeners, listener)

    return listener
}

// Thread safe implementation
func (b *threadSafeBraodcaster) Close() {
    defer b.mutex.Unlock()
    b.mutex.Lock()

    b.baseBroadcaster.Close()
}

func (b *threadSafeBraodcaster) Send(value interface{}) {
    defer b.mutex.Unlock()
    b.mutex.Lock()

    b.baseBroadcaster.Send(value)
}

func (b *threadSafeBraodcaster) Listen(bufSize int) <-chan interface{} {
    defer b.mutex.Unlock()
    b.mutex.Lock()

    return b.baseBroadcaster.Listen(bufSize)
}
