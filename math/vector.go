package math

type Vector []Float

func Dot(a, b Vector) Float {
    assertSameDim(&a, &b)

    // if len(a) >= parallelThreshold {
    //     return parallelReduce(a, b, numRoutines, 1, func(a, b []float64, result []chan float64) {
    //         result[0] <-dot(a, b)
    //     })[0]
    // }
    
    return dot(a, b)
}

func dot(a, b []Float) Float {
    var dot Float
    for i := 0; i < len(a); i++ {
        dot += a[i] * b[i]
    }
    return dot
}

func Length(a Vector) Float {
    return Sqrt(Dot(a, a))
}

func VectorAdd(a, b Vector) Vector {
    assertSameDim(&a, &b)

    result := make(Vector, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] + b[i]
    }
    return result
}
