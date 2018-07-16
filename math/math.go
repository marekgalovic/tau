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

const MaxFloat = float32(goMath.MaxFloat32)

func Abs(x float32) float32 {
    return float32(goMath.Abs(float64(x)))
}

func Pow(x, power float32) float32 {
    // Slow
    return float32(goMath.Pow(float64(x), float64(power)))
}

func Square(x float32) float32 {
    return x * x
}

func Sqrt(x float32) float32 {
    return float32(goMath.Sqrt(float64(x)))
}

func Log(x float32) float32 {
    return float32(goMath.Log(float64(x)))
}

func Trunc(x float32) int {
    return int(goMath.Trunc(float64(x)))
}

func Min(values ...float32) float32 {
    min := MaxFloat
    for _, value := range values {
        if value < min {
            min = value
        }
    }
    return min
}

func Max(values ...float32) float32 {
    max := -MaxFloat
    for _, value := range values {
        if value > max {
            max = value
        }
    }
    return max
}

func EquidistantPlane(a, b Vector) Vector {
    assertSameDim(&a, &b)

    normal := make(Vector, len(a))
    var d float32
    for i := 0; i < len(a); i++ {
        normal[i] = (b[i] - a[i])
        d += normal[i] * ((a[i] + b[i]) / 2)
    }

    return append(normal, d)
}

func PointPlaneDistance(point, plane Vector) float32 {
    if len(point) != len(plane) - 1 {
        panic("Plane vector must be have one more dimension than point.")
    }

    var dot, normalNorm float32
    for i := 0; i < len(point); i++ {
        dot += point[i] * plane[i]
        normalNorm += Square(plane[i])
    }

    return (dot - plane[len(plane) - 1]) / (Sqrt(normalNorm) + 1e-12)
}
