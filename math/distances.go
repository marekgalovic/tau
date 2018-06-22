package math

var SupportedDistanceMetrics = []string{"Euclidean", "Manhattan", "Cosine"}

func EuclideanDistance(a, b Vector) Float {
    assertSameDim(&a, &b)

    var distance Float
    for i := 0; i < len(a); i++ {
        distance += Square(a[i] - b[i])
    }
    
    return Sqrt(distance)
}

func ManhattanDistance(a, b Vector) Float {
    assertSameDim(&a, &b)

    var distance Float
    for i := 0; i < len(a); i++ {
        distance += Abs(a[i] - b[i])
    }

    return distance
}

func CosineDistance(a, b Vector) Float {
    assertSameDim(&a, &b)

    var dot Float
    var aNorm Float
    var bNorm Float
    for i := 0; i < len(a); i++ {
        dot += a[i] * b[i]
        aNorm += Square(a[i])
        bNorm += Square(b[i])
    }

    return dot / (Sqrt(aNorm) * Sqrt(bNorm))
}
