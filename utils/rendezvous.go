// Rendezvous (highest random weight) hashing
package utils

import (
    "math";
    "hash/crc32";
    "hash/crc64";
)

var crc64Table *crc64.Table = crc64.MakeTable(crc32.Castagnoli)

func RendezvousHashScore(node, key string, weight float64) float64 {
    sum := crc64.Checksum(append([]byte(node), []byte(key)...), crc64Table)

    return weight * -math.Log(float64(sum) / 0xFFFFFFFF)
}
