package math

func assertSameDim(i, j *Vector) {
    if len(*i) != len(*j) {
        panic("Vector sizes do not match.")
    }   
}

func parallelReduce(a, b Vector, nRoutines, nResults int, reduceFunc func([]float64, []float64, []chan float64)) []float64 {
    result := make([]chan float64, nResults)
    for i := 0; i < nResults; i++ {
        result[i] = make(chan float64)
    }

    sliceSize := int(len(a) / nRoutines)
    var lb, ub int
    for t := 0; t < nRoutines; t++ {
        lb = t * sliceSize
        ub = lb + sliceSize
        if t == nRoutines - 1 {
            ub += len(a) - (nRoutines * sliceSize)
        }

        go reduceFunc(a[lb:ub], b[lb:ub], result)
    }

    reduced := make([]float64, nResults)
    for t := 0; t < nRoutines; t++ {
        for i := 0; i < nResults; i++ {
            reduced[i] += <-result[i]
        }
    }

    for i := 0; i < nResults; i++ {
        close(result[i])
    }

    return reduced
}
