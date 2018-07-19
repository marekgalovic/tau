package main

import (
    "context";
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

    // "github.com/marekgalovic/tau";
    "github.com/marekgalovic/tau/index";
    pb "github.com/marekgalovic/tau/protobuf";
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
    vorIndex := index.NewVoronoiIndex(d, "Euclidean", 30, 512)

    startAt := time.Now()
    f, err = os.Open("./examples/data/random_normal.csv")
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
        for i, b := range bytes.Fields(lineBytes)[1:] {
            f, _ := strconv.ParseFloat(string(b), 64)
            vec[i] = float32(f)
            if goMath.IsNaN(f) {
                panic("NaN")
            }
        }
        for k := 0; k < 1; k++ {
            // vorIndex.Add(itemIdx, vec)
            vorIndex.Add(int64(itemIdx), tauMath.VectorAdd(vec, tauMath.RandomStandardNormalVector(d)))
            itemIdx++
        }
        // fmt.Println("vorIndex size:", vorIndex.ByteSize() / 1024 / 1024)
    }
    fmt.Println("Data read time:", time.Since(startAt))
    fmt.Println("Item:", vorIndex.Len())

    var totalDuration time.Duration
    for i := 0; i < 1; i++ {
        startAt = time.Now()
        vorIndex.Build(context.Background())
        d := time.Since(startAt)
        // fmt.Println("Build time:", vorIndex.Len(), d)
        totalDuration += d
    }
    fmt.Println("Avg build duration", totalDuration / 1)

    query := vorIndex.Get(0)

    startAt = time.Now()
    bfResults := make(index.SearchResult, 0, vorIndex.Len())
    for idx, item := range vorIndex.Items() {
        distance := tauMath.EuclideanDistance(item, query)
        if idx == 0 {
            fmt.Println(idx, distance)
        }
        bfResults = append(bfResults, &pb.SearchResultItem {
            Id: idx,
            Distance: distance,
        })
    }
    sort.Sort(bfResults)
    fmt.Println("Brute force search time", time.Since(startAt))

    var totalSearchDuration time.Duration
    var result index.SearchResult
    for i := 0; i < 1000; i++ {
        startAt = time.Now()
        result = vorIndex.Search(context.Background(), query)
        d := time.Since(startAt)
        // fmt.Println("Btree search time:", d)
        totalSearchDuration += d
        sort.Sort(result)
    }
    fmt.Println("Avg search time:", totalSearchDuration / 1000)
    fmt.Println("Returned results:", len(result))

    topTenBtreeIds := make(map[int64]struct{})
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

    // vorIndex.Save("foo")
}
