package index

import (
    "io";
    "context";
    "sync";
    "time";
    "runtime";

    "github.com/marekgalovic/tau/math";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    progressBar "gopkg.in/cheggaaa/pb.v1";

    log "github.com/Sirupsen/logrus";
)

type hnswSearchAlgorithm int

const (
    HnswSearchSimple hnswSearchAlgorithm = iota
    HnswSearchHeuristic
)

func (a hnswSearchAlgorithm) String() string {
    names := [...]string {
        "Simple",
        "Heuristic",
    }

    return names[a]
}

// Options
type HnswOption interface {
    apply(*hnswConfig)
}

type hnswOption struct {
    applyFunc func(*hnswConfig)
}

func (opt *hnswOption) apply(config *hnswConfig) {
    opt.applyFunc(config)
}

func HnswLevelMultiplier(value float32) HnswOption {
    return &hnswOption{func(config *hnswConfig) {
        config.levelMultiplier = value
    }}
}

func HnswEf(value int) HnswOption {
    return &hnswOption{func(config *hnswConfig) {
        config.ef = value
    }}
}

func HnswEfConstruction(value int) HnswOption {
    return &hnswOption{func(config *hnswConfig) {
        config.efConstruction = value
    }}
}

func HnswM(value int) HnswOption {
    return &hnswOption{func(config *hnswConfig) {
        config.m = value
    }}
}

func HnswMmax(value int) HnswOption {
    return &hnswOption{func(config *hnswConfig) {
        config.mMax = value
    }}
}

func HnswMmax0(value int) HnswOption {
    return &hnswOption{func(config *hnswConfig) {
        config.mMax0 = value
    }}
}

func HnswSearchAlgorithm(value hnswSearchAlgorithm) HnswOption {
    return &hnswOption{func(config *hnswConfig) {
        config.searchAlgorithm = value
    }}
}

type hnswConfig struct {
    searchAlgorithm hnswSearchAlgorithm
    levelMultiplier float32
    ef int
    efConstruction int
    m int
    mMax int
    mMax0 int
}

type hnswIndex struct {
    baseIndex
    config *hnswConfig
    maxLevel int
    maxLevelMutex *sync.Mutex
    entrypoint *hnswVertex
}

type hnswEdgeSet map[*hnswVertex]float32

type hnswVertex struct {
    id int64
    level int
    edges []hnswEdgeSet
    edgeMutexes []*sync.RWMutex
}

func NewHnswIndex(size int, space math.Space, options ...HnswOption) *hnswIndex {
    config := &hnswConfig {
        searchAlgorithm: HnswSearchSimple,
        levelMultiplier: -1,
        ef: 20,
        efConstruction: 200,
        m: 16,
        mMax: -1,
        mMax0: -1,
    }

    for _, option := range options {
        option.apply(config)
    }

    if config.levelMultiplier == -1 {
        config.levelMultiplier = 1.0 / math.Log(float32(config.m))
    }

    if config.mMax == -1 {
        config.mMax = config.m
    }

    if config.mMax0 == -1 {
        config.mMax0 = 2 * config.m
    }

    log.Infof(
        "HNSW(levelMultiplier=%.4f, ef=%d, efConstruction=%d, m=%d, mMax=%d, mMax0=%d, searchAlgorithm=%s)",
        config.levelMultiplier, config.ef, config.efConstruction, config.m, config.mMax, config.mMax0, config.searchAlgorithm,
    )

    return &hnswIndex{
        baseIndex: newBaseIndex(size, space),
        config: config,
        maxLevelMutex: &sync.Mutex{},
    }
}

func newHnswVertex(id int64, level int) *hnswVertex {
    vertex := &hnswVertex {
        id: id,
        level: level,
        edges: make([]hnswEdgeSet, level + 1),
        edgeMutexes: make([]*sync.RWMutex, level + 1),
    }

    for i := 0; i <= level; i++ {
        vertex.edges[i] = make(hnswEdgeSet)
        vertex.edgeMutexes[i] = &sync.RWMutex{}
    }

    return vertex
}

// Vertex
func (v *hnswVertex) edgesCount(level int) int {
    defer v.edgeMutexes[level].Unlock()
    v.edgeMutexes[level].Lock()

    return len(v.edges[level])
}

func (v *hnswVertex) addEdge(level int, edge *hnswVertex, distance float32) {
    defer v.edgeMutexes[level].Unlock()
    v.edgeMutexes[level].Lock()

    v.edges[level][edge] = distance
}

func (v *hnswVertex) getEdges(level int) hnswEdgeSet {
    defer v.edgeMutexes[level].RUnlock()
    v.edgeMutexes[level].RLock()

    return v.edges[level]
}

func (v *hnswVertex) setEdges(level int, edges hnswEdgeSet) {
    defer v.edgeMutexes[level].Unlock()
    v.edgeMutexes[level].Lock()

    v.edges[level] = edges
}

// Index
func (index *hnswIndex) Save(writer io.Writer) error {
    return nil
}

func (index *hnswIndex) Load(reader io.Reader) error {
    return nil
}

func (index *hnswIndex) Search(ctx context.Context, k int, query math.Vector) SearchResult {
    entrypoint := index.entrypoint
    minDistance := index.space.Distance(query, index.Get(index.entrypoint.id))
    for l := index.maxLevel; l > 0; l-- {
        entrypoint, minDistance = index.greedyClosestNeighbor(query, entrypoint, minDistance, l)
    }

    ef := int(math.Max(float32(index.config.ef), float32(k)))
    neighbors := index.searchLevel(query, entrypoint, ef, 0).Reverse()

    if neighbors.Len() < k {
        k = neighbors.Len()
    }

    result := make(SearchResult, k)
    for i := 0; i < k; i++ {
        item := neighbors.Pop()
        result[i] = &pb.SearchResultItem {
            Id: item.Value().(*hnswVertex).id,
            Distance: item.Priority(),
        }
    }

    return result
}

func (index *hnswIndex) Build(ctx context.Context) {
    numThreads := runtime.NumCPU()
    workQueue := make(chan int64)

    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    for t := 0; t < numThreads; t++ {
        go func(ids chan int64, ctx context.Context) {
            for {
                select {
                case id := <- ids:
                    index.addToGraph(id)
                case <- ctx.Done():
                    return
                }
            }
        }(workQueue, ctx)
    }

    bar := progressBar.StartNew(index.Len())
    bar.SetRefreshRate(10 * time.Second)

    for id, _ := range index.items {
        bar.Increment()
        if index.entrypoint == nil {
            index.entrypoint = newHnswVertex(id, 0)
            continue
        }

        select {
        case workQueue <- id:
        case <- ctx.Done():
            return
        }
    }

    bar.Finish()
}

func (index *hnswIndex) addToGraph(id int64) {
    level := index.randomLevel()

    index.maxLevelMutex.Lock()
    maxLevel := index.maxLevel
    if level > maxLevel {
        defer index.maxLevelMutex.Unlock()
    } else {
        index.maxLevelMutex.Unlock()
    }

    vertex := newHnswVertex(id, level)
    query := index.Get(id)

    entrypoint := index.entrypoint
    minDistance := index.space.Distance(query, index.Get(index.entrypoint.id))
    for l := maxLevel; l > level; l-- {
        entrypoint, minDistance = index.greedyClosestNeighbor(query, entrypoint, minDistance, l)
    }

    for l := int(math.Min(float32(index.maxLevel), float32(level))); l >= 0; l-- {
        neighbors := index.searchLevel(query, entrypoint, index.config.efConstruction, l)

        switch index.config.searchAlgorithm {
        case HnswSearchSimple:
            neighbors = index.selectNeighbors(neighbors, index.config.m)
        case HnswSearchHeuristic:
            neighbors = index.selectNeighborsHeuristic(query, neighbors, index.config.m, l, true, true)
        }

        for neighbors.Len() > 0 {
            item := neighbors.Pop()
            neighbor := item.Value().(*hnswVertex)
            entrypoint = neighbor

            vertex.addEdge(l, neighbor, item.Priority())
            neighbor.addEdge(l, vertex, item.Priority())

            mMax := index.config.mMax
            if l == 0 {
                mMax = index.config.mMax0
            }
            if neighbor.edgesCount(l) > mMax {
                index.pruneNeighbors(neighbor, mMax, l)
            }
        }
    }

    if level > index.maxLevel {
        index.maxLevel = level
        index.entrypoint = vertex
    }
}

func (index *hnswIndex) randomLevel() int {
    return math.Floor(math.RandomExponential(index.config.levelMultiplier))
}

func (index *hnswIndex) greedyClosestNeighbor(query math.Vector, entrypoint *hnswVertex, minDistance float32, level int) (*hnswVertex, float32) {
    for {
        var closestNeighbor *hnswVertex

        entrypoint.edgeMutexes[level].RLock()
        for neighbor, _ := range entrypoint.edges[level] {
            if distance := index.space.Distance(query, index.Get(neighbor.id)); distance < minDistance {
                minDistance = distance
                closestNeighbor = neighbor
            }
        }
        entrypoint.edgeMutexes[level].RUnlock()

        if closestNeighbor == nil {
            break
        }

        entrypoint = closestNeighbor
    }

    return entrypoint, minDistance
}

func (index *hnswIndex) searchLevel(query math.Vector, entrypoint *hnswVertex, ef, level int) utils.PriorityQueue {
    entrypointDistance := index.space.Distance(query, index.Get(entrypoint.id))

    pqItem := utils.NewPriorityQueueItem(entrypointDistance, entrypoint)
    candidateVertices := utils.NewMinPriorityQueue(pqItem)
    resultVertices := utils.NewMaxPriorityQueue(pqItem)

    visitedVertices := make(map[*hnswVertex]struct{}, ef * index.config.mMax0)
    visitedVertices[entrypoint] = struct{}{}

    for candidateVertices.Len() > 0 {
        candidateItem := candidateVertices.Pop()
        candidate := candidateItem.Value().(*hnswVertex)
        lowerBound := resultVertices.Peek().Priority()

        if candidateItem.Priority() > lowerBound {
            break   
        }

        candidate.edgeMutexes[level].RLock()
        for neighbor, _ := range candidate.edges[level] {
            if _, exists := visitedVertices[neighbor]; exists {
                continue
            }
            visitedVertices[neighbor] = struct{}{}

            distance := index.space.Distance(query, index.Get(neighbor.id))
            if (distance < lowerBound) || (resultVertices.Len() < ef) {
                pqItem := utils.NewPriorityQueueItem(distance, neighbor)
                candidateVertices.Push(pqItem)
                resultVertices.Push(pqItem)

                if resultVertices.Len() > ef {
                    resultVertices.Pop()
                }
            }
        }
        candidate.edgeMutexes[level].RUnlock()
    }

    // MaxPriorityQueue
    return resultVertices
}

func (index *hnswIndex) selectNeighbors(neighbors utils.PriorityQueue, k int) utils.PriorityQueue {
    for neighbors.Len() > k {
        neighbors.Pop()
    }

    return neighbors
}

func (index *hnswIndex) selectNeighborsHeuristic(query math.Vector, neighbors utils.PriorityQueue, k, level int, extendCandidates, keepPruned bool) utils.PriorityQueue {
    candidateVertices := neighbors.Reverse()  // MinPriorityQueue

    existingCandidatesSize := neighbors.Len()
    if extendCandidates {
        existingCandidatesSize += neighbors.Len() * index.config.mMax0
    }
    existingCandidates := make(map[*hnswVertex]struct{}, existingCandidatesSize)
    for _, neighbor := range neighbors.ToSlice() {
        existingCandidates[neighbor.Value().(*hnswVertex)] = struct{}{}
    }

    if extendCandidates {
        for neighbors.Len() > 0 {
            candidate := neighbors.Pop().Value().(*hnswVertex)

            candidate.edgeMutexes[level].RLock()
            for neighbor, _ := range candidate.edges[level] {
                if _, exists := existingCandidates[neighbor]; exists {
                    continue
                }
                existingCandidates[neighbor] = struct{}{}

                distance := index.space.Distance(query, index.Get(neighbor.id))
                candidateVertices.Push(utils.NewPriorityQueueItem(distance, neighbor))
            }
            candidate.edgeMutexes[level].RUnlock()
        }
    }

    result := utils.NewMaxPriorityQueue()
    for (candidateVertices.Len() > 0) && (result.Len() < k) {
        result.Push(candidateVertices.Pop())
    }

    if keepPruned {
        for candidateVertices.Len() > 0 {
            result.Push(candidateVertices.Pop())

            if result.Len() >= k {
                break
            }
        }
    }

    return result
}

func (index *hnswIndex) pruneNeighbors(vertex *hnswVertex, k, level int) {
    neighborsQueue := utils.NewMaxPriorityQueue()

    vertex.edgeMutexes[level].RLock()
    for neighbor, distance := range vertex.edges[level] {
        neighborsQueue.Push(utils.NewPriorityQueueItem(distance, neighbor))
    }
    vertex.edgeMutexes[level].RUnlock()

    switch index.config.searchAlgorithm {
    case HnswSearchSimple:
        neighborsQueue = index.selectNeighbors(neighborsQueue, index.config.m)
    case HnswSearchHeuristic:
        query := index.Get(vertex.id)
        neighborsQueue = index.selectNeighborsHeuristic(query, neighborsQueue, index.config.m, level, true, true)
    }

    newNeighbors := make(hnswEdgeSet, neighborsQueue.Len())
    for _, item := range neighborsQueue.ToSlice() {
        newNeighbors[item.Value().(*hnswVertex)] = item.Priority()
    }

    vertex.setEdges(level, newNeighbors)
}
