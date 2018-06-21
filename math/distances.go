package math

var SupportedDistanceMetrics = []string{"Euclidean", "Manhattan", "Cosine"}

func EuclideanDistance(a, b []float32) float32 {
    assertSameDim(&a, &b)

    var distance float32
    for i := 0; i < len(a); i++ {
        distance += Square(a[i] - b[i])
    }
    return Sqrt(distance)
}

func ManhattanDistance(a, b []float32) float32 {
    assertSameDim(&a, &b)

    var distance float32
    for i := 0; i < len(a); i++ {
        distance += Abs(a[i] - b[i])
    }

    return distance
}

func CosineDistance(a, b []float32) float32 {
    assertSameDim(&a, &b)

    var dot float32
    var aNorm float32
    var bNorm float32
    for i := 0; i < len(a); i++ {
        dot += a[i] * b[i]
        aNorm += Square(a[i])
        bNorm += Square(b[i])
    }

    return dot / (Sqrt(aNorm) * Sqrt(bNorm))
}
