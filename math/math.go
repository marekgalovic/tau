package math

import (
    goMath "math";
)

var (
    parallelThreshold = 100000
    numRoutines = 4
)

func SetParallelThreshold(threshold int) { parallelThreshold = threshold }

func SetNumRoutines(n int) { numRoutines = n }

func Square(x float64) float64 {
    return x * x
}

func EquidistantPlane(a, b Vector) Vector {
    assertSameDim(&a, &b)

    normal := make(Vector, len(a))
    var d float64
    for i := 0; i < len(a); i++ {
        normal[i] = (b[i] - a[i])
        d += normal[i] * ((a[i] + b[i]) / 2)
    }

    return append(normal, d)
}

func PointPlaneDistance(point, plane []float64) float64 {
    if len(point) != len(plane) - 1 {
        panic("Plane vector must be have one more dimension than point.")
    }

    var dot, normalNorm float64
    for i := 0; i < len(point); i++ {
        dot += point[i] * plane[i]
        normalNorm += Square(plane[i])
    }

    return (dot - plane[len(plane) - 1]) / (goMath.Sqrt(normalNorm) + 1e-12)
}
