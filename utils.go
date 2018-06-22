package tau

import (
    "math/rand";
)

func sampleDistinctInts(n, max int) []int {
    result := make([]int, n)
    result[0] = rand.Intn(max)
    
    i := 1
    for i < n {
        candidate := rand.Intn(max)
        if candidate != result[i - 1] {
            result[i] = candidate
            i++
        }
    }

    return result
}
