package utils

import (
    "testing";

    "github.com/stretchr/testify/assert";
)

func TestStack(t *testing.T) {
    stack := NewStack()

    assert.Equal(t, stack.Len(), 0)

    stack.Push("foo")
    stack.Push("bar")
    stack.Push("baz")

    assert.Equal(t, stack.Len(), 3)

    assert.Equal(t, "baz", stack.Pop().(string))
    assert.Equal(t, "bar", stack.Pop().(string))
    assert.Equal(t, "foo", stack.Pop().(string))
}

func TestEmptyStackPanicsOnPopAttempt(t *testing.T) {
    defer func() {
        if r := recover(); r == nil {
            t.Error("Did not panic")
        }
    }()

    stack := NewStack()
    stack.Pop()
}
