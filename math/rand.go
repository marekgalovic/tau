package math

import (
    "math/rand";
)

func RandomDistinctInts(n, max int) []int {
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

func RandomUniformVector(size int) Vector {
    vec := make(Vector, size)
    for i := 0; i < size; i++ {
        vec[i] = Float(rand.Float32())
    }
    return vec
}

func RandomStandardNormalVector(size int) Vector {
    vec := make(Vector, size)
    for i := 0; i < size; i++ {
        vec[i] = Float(rand.NormFloat64())
    }
    return vec
}

func RandomNormalVector(size int, mu, sigma Float) Vector {
    vec := make(Vector, size)
    for i := 0; i < size; i++ {
        vec[i] = Float(rand.NormFloat64()) * sigma + mu
    }
    return vec 
}
