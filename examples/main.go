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
    index := tau.NewBtreeIndex(d, "Euclidean", 15, 256)

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
        vec := make([]tauMath.Float, 256)
        for i, b := range bytes.Fields(lineBytes) {
            f, _ := strconv.ParseFloat(string(b), 64)
            vec[i] = tauMath.Float(f)
            if goMath.IsNaN(f) {
                panic("NaN")
            }
        }
        for k := 0; k < 1000; k++ {
            // index.Add(itemIdx, vec)
            index.Add(itemIdx, tauMath.VectorAdd(vec, tauMath.RandomNormalVector(d, 0, 1)))
            itemIdx++
        }
        // fmt.Println("Index size:", index.ByteSize() / 1024 / 1024)
    }
    fmt.Println("Data read time:", time.Since(startAt))

    var totalDuration time.Duration
    for i := 0; i < 1; i++ {
        startAt = time.Now()
        index.Build()
        d := time.Since(startAt)
        // fmt.Println("Build time:", index.Len(), d)
        totalDuration += d
    }
    fmt.Println("Avg build duration", totalDuration / 1)

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

    var totalSearchDuration time.Duration
    var result tau.SearchResult
    for i := 0; i < 1000; i++ {
        startAt = time.Now()
        result = index.Search(query)
        d := time.Since(startAt)
        // fmt.Println("Btree search time:", d)
        totalSearchDuration += d
    }
    fmt.Println("Avg search time:", totalSearchDuration / 1000)
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
