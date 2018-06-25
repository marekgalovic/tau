package main

import (
    "fmt";
    "time";
    // "runtime";

    "github.com/marekgalovic/tau/math"
)

func main() {
    // runtime.GOMAXPROCS(8)
    a := math.RandomStandardNormalVector(1000)

    startAt := time.Now()
    math.VectorAdd(a, a)
    fmt.Println("Time:", time.Since(startAt))

    a = math.Vector{0, 1, 2, 3}
    b := math.Vector{0, 1, 2, 3}
    c := math.VectorAdd(a, b)

    fmt.Println(a)
    fmt.Println(b)
    fmt.Println(c)
}
