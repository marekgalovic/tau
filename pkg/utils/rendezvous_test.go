package utils

import (
    "testing";

    "github.com/stretchr/testify/assert";
)

func TestRendezvousHashScore(t *testing.T) {
    scoreA := RendezvousHashScore("nodeA", "keyA", 1)
    scoreB := RendezvousHashScore("nodeA", "keyA", 1)
    scoreC := RendezvousHashScore("nodeA", "keyA", 1)

    assert.Equal(t, scoreA, scoreB)
    assert.Equal(t, scoreA, scoreC)
}

func TestWeightedRendezvousHashScore(t *testing.T) {
    scoreA := RendezvousHashScore("nodeA", "keyA", 1)
    scoreB := RendezvousHashScore("nodeA", "keyA", 10)

    assert.True(t, scoreB > scoreA)
}
