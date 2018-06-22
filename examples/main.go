package main

import (
    "fmt";
    "time";
    "sort";
    "os";
    "bufio";
    "bytes";
    "strconv";
    "math/rand";
    goMath "math";
    "runtime/pprof";

    "github.com/marekgalovic/tau";
    tauMath "github.com/marekgalovic/tau/math"
)

func main() {
    f, err := os.Create("cpu.prof")
    if err != nil {
        panic(err)
    }
    if err = pprof.StartCPUProfile(f); err != nil {
        panic(err)
    }
    defer pprof.StopCPUProfile()
    d := 256
    // n := 100
    rand.Seed(time.Now().Unix())
    fmt.Println("Tau")
    index := tau.NewBtreeIndex(d, "Euclidean", 1, 128)

    startAt := time.Now()
    f, err = os.Open("./examples/data/dim256.txt")
    if err != nil {
        panic(err)
    }
    reader := bufio.NewReader(f)
    itemIdx := 0
    for {
        lineBytes, _, err := reader.ReadLine()
        if err != nil {
            break
        }
        vec := make([]float32, 256)
        for i, b := range bytes.Fields(lineBytes) {
            f, _ := strconv.ParseFloat(string(b), 32)
            vec[i] = float32(f)
            if goMath.IsNaN(float64(vec[i])) {
                panic("NaN")
            }
        }
        for k := 0; k < 10; k++ {
            index.Add(itemIdx, vec)
            // index.Add(itemIdx, tauMath.VectorAdd(vec, tauMath.RandomStandardNormalVector(d)))
            itemIdx++
        }
    }
    fmt.Println("Data read time:", time.Since(startAt))

    startAt = time.Now()
    index.Build()
    fmt.Println("Build time:", index.Len(), time.Since(startAt))

    query := index.Get(0)

    startAt = time.Now()
    bfResults := make(tau.SearchResult, 0, index.Len())
    for idx, item := range index.Items() {
        distance := tauMath.EuclideanDistance(item, query)
        if idx == 0 {
            fmt.Println(idx, distance)
        }
        bfResults = append(bfResults, tau.SearchResultItem {
            Id: idx,
            Distance: distance,
        })
    }
    sort.Sort(bfResults)
    fmt.Println("Brute force search time", time.Since(startAt))

    startAt = time.Now()
    result := index.Search(query)
    fmt.Println("Btree search time:", time.Since(startAt))
    fmt.Println("Returned results:", len(result))

    topTenBtreeIds := make(map[int]struct{})
    for i := 0; i < 10; i++ {
        if i < len(result) {
            topTenBtreeIds[result[i].Id] = struct{}{}
        }
    }

    var present float32
    for _, resultItem := range bfResults[:10] {
        if _, exists := topTenBtreeIds[resultItem.Id]; exists {
            present += 1
        }
    }
    fmt.Println("Top 10 accuracy:", present / 10.0)

    fmt.Println(bfResults[:10])
    fmt.Println(topTenBtreeIds)

    // index.Save("foo")
}
