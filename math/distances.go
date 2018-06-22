package math

import (
    goMath "math";
)

var SupportedDistanceMetrics = []string{"Euclidean", "Manhattan", "Cosine"}

func EuclideanDistance(a, b Vector) float64 {
    assertSameDim(&a, &b)

    var distance float64
    for i := 0; i < len(a); i++ {
        distance += Square(a[i] - b[i])
    }
    
    return goMath.Sqrt(distance)
}

func ManhattanDistance(a, b Vector) float64 {
    assertSameDim(&a, &b)

    var distance float64
    for i := 0; i < len(a); i++ {
        distance += goMath.Abs(a[i] - b[i])
    }

    return distance
}

func CosineDistance(a, b Vector) float64 {
    assertSameDim(&a, &b)

    var dot float64
    var aNorm float64
    var bNorm float64
    for i := 0; i < len(a); i++ {
        dot += a[i] * b[i]
        aNorm += Square(a[i])
        bNorm += Square(b[i])
    }

    return dot / (goMath.Sqrt(aNorm) * goMath.Sqrt(bNorm))
}
