package tau

import (
    "math/rand";
)

func sampleDistinctInts(n int) (int, int) {
    idsEqual := true
    var aIdx, bIdx int

    for idsEqual {
        aIdx = rand.Intn(n)
        bIdx = rand.Intn(n)
        idsEqual = aIdx == bIdx
    }

    return aIdx, bIdx
}
