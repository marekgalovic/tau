package math

type Vector []Float

func (v Vector) Len() int { return len(v) }

func (v Vector) Swap(i, j int) { v[i], v[j] = v[j], v[i] }

func (v Vector) Less(i, j int) bool { return v[i] < v[j] }

func Dot(a, b Vector) Float {
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

func VectorSubtract(a, b Vector) Vector {
    assertSameDim(&a, &b)

    result := make(Vector, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] - b[i]
    }
    return result
}

func VectorMultiply(a, b Vector) Vector {
    assertSameDim(&a, &b)

    result := make(Vector, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] * b[i]
    }
    return result
}

func VectorDivide(a, b Vector) Vector {
    assertSameDim(&a, &b)

    result := make(Vector, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] / b[i]
    }
    return result
}

func VectorScalarAdd(a Vector, b Float) Vector {
    result := make(Vector, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] + b
    }
    return result
}

func VectorScalarSubtract(a Vector, b Float) Vector {
    result := make(Vector, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] - b
    }
    return result
}

func VectorScalarMultiply(a Vector, b Float) Vector {
    result := make(Vector, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] * b
    }
    return result
}

func VectorScalarDivide(a Vector, b Float) Vector {
    result := make(Vector, len(a))
    for i := 0; i < len(a); i++ {
        result[i] = a[i] / b
    }
    return result
}
