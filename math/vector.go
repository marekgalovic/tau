package math

import (
    goMath "math";
    "math/rand";
)

type Vector []float64

func Dot(a, b Vector) float64 {
    assertSameDim(&a, &b)

    if len(a) >= ParallelThreshold {
        return parallelReduce(a, b, NumRoutines, 1, func(a, b []float64, result []chan float64) {
            result[0] <-dot(a, b)
        })[0]
    }
    return dot(a, b)
}

func dot(a, b []float64) float64 {
    var dot float64
    for i := 0; i < len(a); i++ {
        dot += a[i] * b[i]
    }
    return dot
}

func VectorAdd(a, b Vector) Vector {
    assertSameDim(&a, &b)

    result := make(Vector, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] + b[i]
    }
    return result
}

func Length(a Vector) float64 {
    return goMath.Sqrt(Dot(a, a))
}

func RandomUniformVector(size int) Vector {
    vec := make(Vector, size)
    for i := 0; i < size; i++ {
        vec[i] = rand.Float64()
    }
    return vec
}

func RandomStandardNormalVector(size int) Vector {
    vec := make(Vector, size)
    for i := 0; i < size; i++ {
        vec[i] = rand.NormFloat64()
    }
    return vec
}

func RandomNormalVector(size int, mu, sigma float64) Vector {
    vec := make(Vector, size)
    for i := 0; i < size; i++ {
        vec[i] = rand.NormFloat64() * sigma + mu
    }
    return vec 
}
