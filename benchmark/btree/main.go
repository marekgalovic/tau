package main

import (
    "os";
    "io";
    "time";
    "encoding/csv";
    "strconv";
    "context";
    "sort";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/index";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    log "github.com/Sirupsen/logrus";
)

func parseLine(line []string) (int64, math.Vector, error) {
    id, err := strconv.ParseInt(line[0], 10, 64)
    if err != nil {
        return 0, nil, err
    }

    vec := make(math.Vector, len(line[1:]))
    for i, raw := range line[1:] {
        val, err := strconv.ParseFloat(raw, 32)
        if err != nil {
            return 0, nil, err
        }
        vec[i] = float32(val)
    }

    return id, vec, nil
}

func main() {
    trainDataFile, err := os.Open("./benchmark/data/sift-128/train.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer trainDataFile.Close()

    btreeIndex := index.NewBtreeIndex(128, "Euclidean", 30, 512)
    trainDataReader := csv.NewReader(trainDataFile)
    start := time.Now()
    for {
        line, err := trainDataReader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Fatal(err)
        }
        id, vec, err := parseLine(line)
        if err != nil {
            log.Fatal(err)
        }
        btreeIndex.Add(id, vec)
    }
    log.Infof("Index load time: %s", time.Since(start))

    start = time.Now()
    btreeIndex.Build(context.Background())
    log.Infof("Index build time: %s", time.Since(start))

    testDataFile, err := os.Open("./benchmark/data/sift-128/test.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer testDataFile.Close()

    testDataReader := csv.NewReader(testDataFile)
    testData := make([]math.Vector, 0)
    start = time.Now()
    for {
        line, err := testDataReader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Fatal(err)
        }
        _, vec, err := parseLine(line)
        if err != nil {
            log.Fatal(err)
        }
        testData = append(testData, vec)
    }
    log.Infof("Test data load time: %s", time.Since(start))

    start = time.Now()
    nns := make([][]*pb.SearchResultItem, len(testData))
    for i, item := range testData {
        neighbors := btreeIndex.Search(context.Background(), item)
        sort.Sort(neighbors)
        k := 100
        if k > len(neighbors) {
            k = len(neighbors)
        }
        nns[i] = neighbors[:k]
    }
    duration := time.Since(start)
    log.Infof("Search time: %s, qps: %.4f", duration, float64(len(nns)) / duration.Seconds())

    neighborsDataFile, err := os.Open("./benchmark/data/sift-128/neighbors.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer neighborsDataFile.Close()

    trueIds := make([]utils.Set, 0)
    neighborsReader := csv.NewReader(neighborsDataFile)
    for {
        line, err := neighborsReader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Fatal(err)
        }
        ids := utils.NewSet()
        for _, raw := range line[1:] {
            id, err := strconv.ParseInt(raw, 10, 64)
            if err != nil {
                log.Fatal(err)
            }
            ids.Add(id)
        }
        trueIds = append(trueIds, ids)
    }
    
    recall := float64(0)
    for i, neighbors := range nns {
        retrievedIds := utils.NewSet()
        for _, n := range neighbors {
            retrievedIds.Add(n.Id)
        }
        recall += (float64(retrievedIds.Intersection(trueIds[i]).Len()) / float64(trueIds[i].Len()))
    }
    log.Infof("Recall: %.8f", recall / float64(len(nns)))
}
