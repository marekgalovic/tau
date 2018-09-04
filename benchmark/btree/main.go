package main

import (
    "os";
    "io";
    "time";
    "encoding/csv";
    "strconv";
    "context";
    "sort";
    "runtime";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/index";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    log "github.com/Sirupsen/logrus";
)

type searchTask struct {
    id int
    query math.Vector
}

type searchTaskResult struct {
    id int
    items []*pb.SearchResultItem
}

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

func searchWorker(ctx context.Context, btreeIndex index.Index, tasks chan searchTask, results chan searchTaskResult) {
    for {
        select {
        case task := <-tasks:
            neighbors := btreeIndex.Search(ctx, 100, task.query)
            sort.Sort(neighbors)
            k := 100
            if k > len(neighbors) {
                k = len(neighbors)
            }
            results <- searchTaskResult{task.id, neighbors[:k]}
        case <-ctx.Done():
            return
        }
    }
}

func main() {
    trainDataFile, err := os.Open("./benchmark/data/sift-128/train.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer trainDataFile.Close()

    btreeIndex := index.NewBtreeIndex(128, math.NewEuclideanSpace(), index.BtreeNumTrees(50), index.BtreeMaxLeafItems(128))
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

    numCPUs := runtime.NumCPU()
    log.Infof("Search threads: %d", numCPUs)
    tasksChan := make(chan searchTask, numCPUs)
    resultChan := make(chan searchTaskResult)
    ctx, stopSearchWorkers := context.WithCancel(context.Background())
    for i := 0; i < numCPUs; i++ {
        go searchWorker(ctx, btreeIndex, tasksChan, resultChan)
    }

    start = time.Now()
    go func() {
        for i, item := range testData {
            tasksChan <- searchTask{i, item}
        }
    }()

    nns := make([][]*pb.SearchResultItem, len(testData))
    n := 0
    for {
        result := <-resultChan
        nns[result.id] = result.items
        n++
        if n == len(testData) {
            break
        }
    }
    stopSearchWorkers()
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
