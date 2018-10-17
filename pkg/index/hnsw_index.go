package index

import (
    "fmt";
    "io";
    "bufio";
    "context";
    "sync";
    "time";
    "runtime";
    "encoding/binary";

    "github.com/marekgalovic/tau/pkg/math";
    pb "github.com/marekgalovic/tau/pkg/protobuf";
    "github.com/marekgalovic/tau/pkg/utils";

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
    vertices []*hnswVertex
    verticesMutex *sync.Mutex
}

type hnswEdgeSet map[*hnswVertex]float32

type hnswVertex struct {
    id int64
    vector math.Vector
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
        vertices: make([]*hnswVertex, 0),
        verticesMutex: &sync.Mutex{},
    }
}

func newHnswVertex(id int64, vector math.Vector, level int) *hnswVertex {
    vertex := &hnswVertex {
        id: id,
        vector: vector,
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
    defer v.edgeMutexes[level].RUnlock()
    v.edgeMutexes[level].RLock()

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
func (index *hnswIndex) ToProto() *pb.Index {
    proto := index.baseIndex.ToProto()
    proto.Options = &pb.Index_Hnsw {
        Hnsw: &pb.HnswIndexOptions {
            SearchAlgorithm: pb.HnswSearchAlgorithm(index.config.searchAlgorithm),
            LevelMultiplier: index.config.levelMultiplier,
            Ef: int32(index.config.ef),
            EfConstruction: int32(index.config.efConstruction),
            M: int32(index.config.m),
            MMax: int32(index.config.mMax),
            MMax_0: int32(index.config.mMax0),
        },
    }

    return proto
}

func (index *hnswIndex) Reset() {
    index.verticesMutex.Lock()
    defer index.verticesMutex.Unlock()
    index.maxLevelMutex.Lock()
    defer index.maxLevelMutex.Unlock()

    index.maxLevel = 0
    index.entrypoint = nil
    index.vertices = make([]*hnswVertex, 0)
    index.baseIndex.Reset()
}

func (index *hnswIndex) Save(writer io.Writer) error {
    if index.entrypoint == nil {
        return fmt.Errorf("Cannot save empty index. Call .Build() first.")
    }

    if err := index.writeHeader(writer, []byte("tauHNW")); err != nil {
        return err
    }

    // Index specific headers
    if err := binary.Write(writer, binary.LittleEndian, int32(index.config.searchAlgorithm)); err != nil {
        return err
    }

    if err := binary.Write(writer, binary.LittleEndian, index.config.levelMultiplier); err != nil {
        return err
    }

    if err := binary.Write(writer, binary.LittleEndian, int32(index.config.ef)); err != nil {
        return err
    }

    if err := binary.Write(writer, binary.LittleEndian, int32(index.config.efConstruction)); err != nil {
        return err
    }

    if err := binary.Write(writer, binary.LittleEndian, int32(index.config.m)); err != nil {
        return err
    }

    if err := binary.Write(writer, binary.LittleEndian, int32(index.config.mMax)); err != nil {
        return err
    }

    if err := binary.Write(writer, binary.LittleEndian, int32(index.config.mMax0)); err != nil {
        return err
    }

    if err := binary.Write(writer, binary.LittleEndian, int32(index.maxLevel)); err != nil {
        return err
    }

    // Index body
    if err := binary.Write(writer, binary.LittleEndian, index.entrypoint.id); err != nil {
        return err
    }

    bufWriter := bufio.NewWriter(writer)
    for _, vertex := range index.vertices {
        if err := binary.Write(bufWriter, binary.LittleEndian, vertex.id); err != nil {
            return err
        }
        if err := binary.Write(bufWriter, binary.LittleEndian, int32(vertex.level)); err != nil {
            return err
        }
        for l := vertex.level; l >= 0; l-- {
            if err := binary.Write(bufWriter, binary.LittleEndian, int32(len(vertex.edges[l]))); err != nil {
                return err
            }

            for neighbor, distance := range vertex.edges[l] {
                if err := binary.Write(bufWriter, binary.LittleEndian, neighbor.id); err != nil {
                    return err
                }
                if err := binary.Write(bufWriter, binary.LittleEndian, distance); err != nil {
                    return err
                }
            }
        }

        if err := bufWriter.Flush(); err != nil {
            return err
        }
        bufWriter.Reset(writer)
    }

    return nil
}

func (index *hnswIndex) Load(reader io.Reader) error {
    indexType, err := index.readHeader(reader)
    if err != nil {
        return err
    }

    if string(indexType) != "tauHNW" {
        return fmt.Errorf("Invalid index type `%s`", indexType)
    }

    // Index specific headers
    var searchAlgorithm int32
    if err := binary.Read(reader, binary.LittleEndian, &searchAlgorithm); err != nil {
        return err
    }
    index.config.searchAlgorithm = hnswSearchAlgorithm(searchAlgorithm)

    if err := binary.Read(reader, binary.LittleEndian, &index.config.levelMultiplier); err != nil {
        return err
    }

    var ef int32
    if err := binary.Read(reader, binary.LittleEndian, &ef); err != nil {
        return err
    }
    index.config.ef = int(ef)

    var efConstruction int32
    if err := binary.Read(reader, binary.LittleEndian, &efConstruction); err != nil {
        return err
    }
    index.config.efConstruction = int(efConstruction)

    var m int32
    if err := binary.Read(reader, binary.LittleEndian, &m); err != nil {
        return err
    }
    index.config.m = int(m)

    var mMax int32
    if err := binary.Read(reader, binary.LittleEndian, &mMax); err != nil {
        return err
    }
    index.config.mMax = int(mMax)

    var mMax0 int32
    if err := binary.Read(reader, binary.LittleEndian, &mMax0); err != nil {
        return err
    }
    index.config.mMax0 = int(mMax0)

    var maxLevel int32
    if err := binary.Read(reader, binary.LittleEndian, &maxLevel); err != nil {
        return err
    }
    index.maxLevel = int(maxLevel)

    // Index body
    var entrypointId int64
    if err := binary.Read(reader, binary.LittleEndian, &entrypointId); err != nil {
        return err
    }

    vertices := make(map[int64]*hnswVertex, index.Len())
    for id, vector := range index.items {
        vertices[id] = &hnswVertex{id: id, vector: vector}
    }
    index.entrypoint = vertices[entrypointId]

    reader = bufio.NewReader(reader)
    index.vertices = make([]*hnswVertex, index.Len())
    for i := 0; i < index.Len(); i++ {
        var id int64
        if err := binary.Read(reader, binary.LittleEndian, &id); err != nil {
            return err
        }
        vertex := vertices[id]
        index.vertices[i] = vertex

        var level int32
        if err := binary.Read(reader, binary.LittleEndian, &level); err != nil {
            return err
        }

        vertex.level = int(level)
        vertex.edges = make([]hnswEdgeSet, level + 1)
        vertex.edgeMutexes = make([]*sync.RWMutex, level + 1)

        for l := level; l >= 0; l-- {
            var numEdges int32
            if err := binary.Read(reader, binary.LittleEndian, &numEdges); err != nil {
                return err
            }

            vertex.edges[l] = make(hnswEdgeSet, numEdges)
            vertex.edgeMutexes[l] = &sync.RWMutex{}

            for j := 0; j < int(numEdges); j++ {
                var neighborId int64
                if err := binary.Read(reader, binary.LittleEndian, &neighborId); err != nil {
                    return err
                }
                var distance float32
                if err := binary.Read(reader, binary.LittleEndian, &distance); err != nil {
                    return err
                }

                vertex.edges[l][vertices[neighborId]] = distance
            }
        }
    }

    return nil
}

func (index *hnswIndex) Search(ctx context.Context, k int, query math.Vector) SearchResult {
    entrypoint := index.entrypoint
    minDistance := index.space.Distance(query, index.entrypoint.vector)
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

    for id, vec := range index.items {
        bar.Increment()
        if index.entrypoint == nil {
            index.entrypoint = newHnswVertex(id, vec, 0)
            index.vertices = append(index.vertices, index.entrypoint)
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

    query := index.Get(id)
    vertex := newHnswVertex(id, query, level)

    index.verticesMutex.Lock()
    index.vertices = append(index.vertices, vertex)
    index.verticesMutex.Unlock()

    entrypoint := index.entrypoint
    minDistance := index.space.Distance(query, index.entrypoint.vector)
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
            if distance := index.space.Distance(query, neighbor.vector); distance < minDistance {
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
    entrypointDistance := index.space.Distance(query, entrypoint.vector)

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

            distance := index.space.Distance(query, neighbor.vector)
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

                distance := index.space.Distance(query, neighbor.vector)
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
        neighborsQueue = index.selectNeighborsHeuristic(vertex.vector, neighborsQueue, index.config.m, level, true, true)
    }

    newNeighbors := make(hnswEdgeSet, neighborsQueue.Len())
    for _, item := range neighborsQueue.ToSlice() {
        newNeighbors[item.Value().(*hnswVertex)] = item.Priority()
    }

    vertex.setEdges(level, newNeighbors)
}
