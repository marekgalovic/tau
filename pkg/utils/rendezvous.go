// Rendezvous (highest random weight) hashing
package utils

import (
    "math";
    
    "github.com/spaolacci/murmur3";
)

func RendezvousHashScore(node, key string, weight float64) float64 {
    hash := murmur3.New32()
    hash.Write([]byte(node))
    hash.Write([]byte(key))

    return weight * -math.Log(float64(hash.Sum32()) / 0xFFFFFFFF)
}
