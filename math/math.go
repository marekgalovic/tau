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

type Float float32

const MaxFloat = Float(goMath.MaxFloat32)

func Abs(x Float) Float {
    return Float(goMath.Abs(float64(x)))
}

func Pow(x, power Float) Float {
    // Slow
    return Float(goMath.Pow(float64(x), float64(power)))
}

func Square(x Float) Float {
    return x * x
}

func Sqrt(x Float) Float {
    return Float(goMath.Sqrt(float64(x)))
}

func Log(x Float) Float {
    return Float(goMath.Log(float64(x)))
}

func Trunc(x Float) int {
    return int(goMath.Trunc(float64(x)))
}

func EquidistantPlane(a, b Vector) Vector {
    assertSameDim(&a, &b)

    normal := make(Vector, len(a))
    var d Float
    for i := 0; i < len(a); i++ {
        normal[i] = (b[i] - a[i])
        d += normal[i] * ((a[i] + b[i]) / 2)
    }

    return append(normal, d)
}

func PointPlaneDistance(point, plane Vector) Float {
    if len(point) != len(plane) - 1 {
        panic("Plane vector must be have one more dimension than point.")
    }

    var dot, normalNorm Float
    for i := 0; i < len(point); i++ {
        dot += point[i] * plane[i]
        normalNorm += Square(plane[i])
    }

    return (dot - plane[len(plane) - 1]) / (Sqrt(normalNorm) + 1e-12)
}
