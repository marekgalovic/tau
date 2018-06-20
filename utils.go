package tau

import (
    "math";
    "math/rand"
)

func assertSameDim(i, j *[]float32) {
    if len(*i) != len(*j) {
        panic("Cannot compute distance for vectors with different number of components")
    }   
}

func euclideanDistance(vec_a, vec_b []float32) float32 {
    assertSameDim(&vec_a, &vec_b)

    var distance float64
    for i := 0; i < len(vec_a); i++ {
        distance += math.Pow(float64(vec_a[i] - vec_b[i]), 2)
    }

    return float32(math.Sqrt(distance))
}

func manhattanDistance(vec_a, vec_b []float32) float32 {
    assertSameDim(&vec_a, &vec_b)

    var distance float64
    for i := 0; i < len(vec_a); i++ {
        distance += math.Abs(float64(vec_a[i] - vec_b[i]))
    }

    return float32(distance)
}

func cosineDistance(vec_a, vec_b []float32) float32 {
    assertSameDim(&vec_a, &vec_b)

    var dot float32
    var a_norm float64
    var b_norm float64
    for i := 0; i < len(vec_a); i++ {
        dot += vec_a[i] * vec_b[i]
        a_norm += math.Pow(float64(vec_a[i]), 2)
        b_norm += math.Pow(float64(vec_b[i]), 2)
    }

    return dot / float32(math.Sqrt(a_norm) * math.Sqrt(b_norm))
}

func randomVector(size int) []float32 {
    item := make([]float32, size)
    for i := 0; i < size; i++ {
        item[i] = rand.Float32()
    }
    return item
}
