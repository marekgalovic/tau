package utils

import (
    "time";
    "testing";

    "github.com/stretchr/testify/assert";
)

func TestBroadcast(t *testing.T) {
    broadcast := NewBroadcast()
    listener := broadcast.Listen(10)

    broadcast.Send("foo")
    broadcast.Send("bar")

    assert.Equal(t, "foo", (<-listener).(string))
    assert.Equal(t, "bar", (<-listener).(string))
}

func TestBroadcastDoesNotBlockOnSend(t *testing.T) {
    broadcast := NewBroadcast()

    done := make(chan bool)
    go func() {
        broadcast.Send("foo")
        done <- true
    }()

    select {
    case <-done:
    case <-time.After(1 * time.Second):
        t.Errorf("Broadcast send blocked")
    }
}

func TestBroadcastWithMultipleListeners(t *testing.T) {
    broadcast := NewBroadcast()
    listenerA := broadcast.Listen(10)
    listenerB := broadcast.Listen(10)
    listenerC := broadcast.Listen(10)

    broadcast.Send("foo")
    broadcast.Send("bar")

    assert.Equal(t, "foo", (<-listenerA).(string))
    assert.Equal(t, "bar", (<-listenerA).(string))
    assert.Equal(t, "foo", (<-listenerB).(string))
    assert.Equal(t, "bar", (<-listenerB).(string))
    assert.Equal(t, "foo", (<-listenerC).(string))
    assert.Equal(t, "bar", (<-listenerC).(string))
}
