package utils

import (
    "testing";

    "github.com/stretchr/testify/assert";
)

func TestMinPriorityQueue(t *testing.T) {
    q := NewMinPriorityQueue()

    q.Push(NewPriorityQueueItem(3, "foo"))
    q.Push(NewPriorityQueueItem(1, "bar"))
    q.Push(NewPriorityQueueItem(2, "bag"))

    assert.Equal(t, "bar", q.Peek().Value())
    assert.Equal(t, "bar", q.Peek().Value())

    assert.Equal(t, "bar", q.Pop().Value())
    assert.Equal(t, "bag", q.Pop().Value())
    assert.Equal(t, "foo", q.Pop().Value())
    assert.Equal(t, 0, q.Len())
}

func TestMaxPriorityQueue(t *testing.T) {
    q := NewMaxPriorityQueue()

    q.Push(NewPriorityQueueItem(3, "foo"))
    q.Push(NewPriorityQueueItem(1, "bar"))
    q.Push(NewPriorityQueueItem(2, "bag"))

    assert.Equal(t, "foo", q.Peek().Value())
    assert.Equal(t, "foo", q.Peek().Value())

    assert.Equal(t, "foo", q.Pop().Value())
    assert.Equal(t, "bag", q.Pop().Value())
    assert.Equal(t, "bar", q.Pop().Value())
    assert.Equal(t, 0, q.Len())
}

func TestPriorityQueueReverse(t *testing.T) {
    q := NewMaxPriorityQueue()

    q.Push(NewPriorityQueueItem(3, "foo"))
    q.Push(NewPriorityQueueItem(1, "bar"))
    q.Push(NewPriorityQueueItem(2, "bag"))

    assert.Equal(t, 3, q.Len())
    assert.Equal(t, "foo", q.Peek().Value())

    q = q.Reverse()

    assert.Equal(t, 3, q.Len())
    assert.Equal(t, "bar", q.Peek().Value())

    assert.Equal(t, "bar", q.Pop().Value())
    assert.Equal(t, "bag", q.Pop().Value())
    assert.Equal(t, "foo", q.Pop().Value())
    assert.Equal(t, 0, q.Len())
}
