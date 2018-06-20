package main

import (
    "fmt";
    "time";
    "math/rand";

    "github.com/marekgalovic/tau"
)

func randomItem(size int) []float32 {
    item := make([]float32, size)
    for i := 0; i < size; i++ {
        item[i] = rand.Float32()
    }
    return item
}

func main() {
    d := 512
    n := 1000000
    // rand.Seed(time.Now().Unix())
    fmt.Println("Tau", time.Now().Unix())
    index := tau.NewBtreeIndex(d, 1, 1000)

    for i := 0; i < n; i++ {
        if err := index.Add(i, randomItem(d)); err != nil {
            panic(err)
        }
    }

    buildStartAt := time.Now()
    index.Build()
    fmt.Println("Build time:", index.Len(), time.Since(buildStartAt))
    // index.Add(1, []float32{0.0, 1.0})

    // fmt.Println(index)
}
