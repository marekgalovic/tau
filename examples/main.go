package main

import (
    "fmt";
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
    fmt.Println("Tau")
    index := tau.VoronoiIndex(4, 100)

    for i := 0; i < 10000; i++ {
        if err := index.Add(i, randomItem(4)); err != nil {
            panic(err)
        }
    }
    index.Build()
    // index.Add(1, []float32{0.0, 1.0})

    // fmt.Println(index)
}
