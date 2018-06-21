package math

import (
    "math";
    "math/rand";
)

const MaxFloat32 = float32(math.MaxFloat32)

func Abs(x float32) float32 {
    return float32(math.Abs(float64(x)))
}

func Pow(x, power float32) float32 {
    return float32(math.Pow(float64(x), float64(power)))
}

func Square(x float32) float32 {
    return Pow(x, 2)
}

func Sqrt(x float32) float32 {
    return float32(math.Sqrt(float64(x)))
}

func Log(x float32) float32 {
    return float32(math.Log(float64(x)))
}

func Trunc(x float32) int {
    return int(math.Trunc(float64(x)))
}

func assertSameDim(i, j *[]float32) {
    if len(*i) != len(*j) {
        panic("Vector sizes do not match.")
    }   
}

func VectorAdd(a, b []float32) []float32 {
    assertSameDim(&a, &b)

    result := make([]float32, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] + b[i]
    }
    return result
}

func VectorScalarAdd(v []float32, x float32) []float32 {
    result := make([]float32, len(v))
    for i := 0; i < len(v); i++ {
        result[i] = v[i] + x
    }
    return result
}

func VectorSubtract(a, b []float32) []float32 {
    assertSameDim(&a, &b)

    result := make([]float32, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] - b[i]
    }
    return result
}

func VectorScalarSubtract(v []float32, x float32) []float32 {
    result := make([]float32, len(v))
    for i := 0; i < len(v); i++ {
        result[i] = v[i] - x
    }
    return result
}

func VectorMultiply(a, b []float32) []float32 {
    assertSameDim(&a, &b)

    result := make([]float32, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] * b[i]
    }
    return result
}

func VectorScalarMultiply(v []float32, x float32) []float32 {
    result := make([]float32, len(v))
    for i := 0; i < len(v); i++ {
        result[i] = v[i] * x
    }
    return result
}

func VectorDivide(a, b []float32) []float32 {
    assertSameDim(&a, &b)

    result := make([]float32, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] / b[i]
    }
    return result
}

func VectorScalarDivide(v []float32, x float32) []float32 {
    result := make([]float32, len(v))
    for i := 0; i < len(v); i++ {
        result[i] = v[i] / x
    }
    return result
}

func VectorDot(a, b []float32) float32 {
    assertSameDim(&a, &b)

    var dot float32
    for i := 0; i < len(a); i++ {
        dot += a[i] * b[i]
    }
    return dot
}

func EquidistantPlane(a, b []float32) []float32 {
    assertSameDim(&a, &b)

    normal := make([]float32, len(a))
    var d float32
    for i := 0; i < len(a); i++ {
        normal[i] = (b[i] - a[i])
        d += normal[i] * ((a[i] + b[i]) / 2)
    }

    return append(normal, d)
}

func PointPlaneDistance(point, plane []float32) float32 {
    if len(point) != len(plane) - 1 {
        panic("Plane vector must be have one more dimension than point.")
    }

    var dot, normalNorm float32
    for i := 0; i < len(point); i++ {
        dot += point[i] * plane[i]
        normalNorm += Square(plane[i])
    }

    return (dot - plane[len(plane) - 1]) / Sqrt(normalNorm)
}

func RandomUniformVector(size int) []float32 {
    vec := make([]float32, size)
    for i := 0; i < size; i++ {
        vec[i] = rand.Float32()
    }
    return vec
}

func RandomStandardNormalVector(size int) []float32 {
    vec := make([]float32, size)
    for i := 0; i < size; i++ {
        vec[i] = float32(rand.NormFloat64())
    }
    return vec
}

func RandomNormalVector(size int, mu, sigma float32) []float32 {
    vec := make([]float32, size)
    for i := 0; i < size; i++ {
        vec[i] = float32(rand.NormFloat64()) * sigma + mu
    }
    return vec 
}
