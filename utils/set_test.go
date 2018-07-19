package utils

import (
    "testing";

    "github.com/stretchr/testify/assert";
)

func TestCreateSetWithElements(t *testing.T) {
    s := NewSet("foo", "bar", "baz")

    assert.Equal(t, s.Len(), 3)
}

func TestSetClear(t *testing.T) {
    s := NewSet("foo", "bar", "baz")

    assert.Equal(t, s.Len(), 3)
    s.Clear()
    assert.Equal(t, s.Len(), 0)
}

func TestSetAdd(t *testing.T) {
    s := NewSet()

    assert.Equal(t, s.Len(), 0)

    s.Add("foo")
    s.Add("bar")

    assert.Equal(t, s.Len(), 2)
}

func TestSetRemove(t *testing.T) {
    s := NewSet("foo", "bar", "baz")

    assert.True(t, s.Contains("foo"))
    s.Remove("foo")
    assert.False(t, s.Contains("foo"))
}

func TestSetContains(t *testing.T) {
    s := NewSet()

    assert.False(t, s.Contains("foo"))
    s.Add("foo")
    assert.True(t, s.Contains("foo"))
}

func TestSetEqual(t *testing.T) {
    s := NewSet("foo", "bar", "baz")
    otherEqual := NewSet("foo", "bar", "baz")
    otherNotEqual := NewSet("foo", "bar", "bag")

    assert.True(t, s.Equal(otherEqual))
    assert.False(t, s.Equal(otherNotEqual))
}

func TestSetDifference(t *testing.T) {
    setA := NewSet("foo", "bar", "baz")
    setB := NewSet("bar", "baz", "bag")

    expected := NewSet("foo")

    assert.True(t, expected.Equal(setA.Difference(setB)))
}

func TestSetUnion(t *testing.T) {
    setA := NewSet("foo", "bar", "baz")
    setB := NewSet("bar", "baz", "bag")

    expected := NewSet("foo", "bar", "baz", "bag")

    assert.True(t, expected.Equal(setA.Union(setB)))
}

func TestSetToSlice(t *testing.T) {
    s := NewSet("foo", "bar", "baz")

    assert.Equal(t, len(s.ToSlice()), 3)
}

func TestSetRand(t *testing.T) {
    item := NewSet("foo", "bar", "baz").Rand()

    assert.NotNil(t, item)
}

func TestEmptySetRand(t *testing.T) {
    item := NewSet().Rand()

    assert.Nil(t, item)
}
