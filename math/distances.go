package math

type Space interface {
    Distance(Vector, Vector) float32
}

type space struct {
    distanceFunc func(Vector, Vector) float32
}

func (s *space) Distance(a, b Vector) float32 {
    return s.distanceFunc(a, b)
}

func NewEuclideanSpace() Space {
    return &space{EuclideanDistance}
}

func EuclideanDistance(a, b Vector) float32 {
    assertSameDim(&a, &b)

    var distance float32
    for i := 0; i < len(a); i++ {
        distance += Square(a[i] - b[i])
    }
    
    return Sqrt(distance)
}

func NewManhattanSpace() Space {
    return &space{ManhattanDistance}
}

func ManhattanDistance(a, b Vector) float32 {
    assertSameDim(&a, &b)

    var distance float32
    for i := 0; i < len(a); i++ {
        distance += Abs(a[i] - b[i])
    }

    return distance
}

func NewCosineSpace() Space {
    return &space{CosineDistance}
}

func CosineDistance(a, b Vector) float32 {
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
