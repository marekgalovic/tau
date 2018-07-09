package utils

import (
    "sync";
)

type Broadcaster interface {
    Close()
    Send(interface{})
    Listen() <-chan interface{}
}

type baseBroadcaster struct {
    listeners []chan interface{}
}

type threadSafeBraodcaster struct {
    baseBroadcaster *baseBroadcaster
    mutex *sync.Mutex
}

func NewBroadcast() Broadcaster {
    return &baseBroadcaster{}
}

func NewThreadSafeBroadcast() Broadcaster {
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

func (b *baseBroadcaster) Listen() <-chan interface{} {
    listener := make(chan interface{})
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

func (b *threadSafeBraodcaster) Listen() <-chan interface{} {
    defer b.mutex.Unlock()
    b.mutex.Lock()

    return b.baseBroadcaster.Listen()
}
