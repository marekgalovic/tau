package index

import (
    "io";
    "context";
    "sync";
    // "sort";
    // "time";

    "github.com/marekgalovic/tau/math";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    // log "github.com/Sirupsen/logrus";
)

type hnswIndex struct {
    baseIndex
    levelMultiplier float32
    efConstruction int
    m int
    maxLevel int
    maxLevelMutex *sync.Mutex
    entrypoint *hnswVertex
}

type hnswVertex struct {
    id int64
    level int
    edges []utils.Set
    edgeMutexes []*sync.RWMutex
}

type hnswVertexDistance struct {
    vertex *hnswVertex
    distance float32
}

func NewHnswIndex(size int, metric string, levelMultiplier, efConstruction, m int) *hnswIndex {
    return &hnswIndex{
        baseIndex: newBaseIndex(size, metric),
        maxLevelMutex: &sync.Mutex{},
        levelMultiplier: float32(levelMultiplier),
        efConstruction: efConstruction,
        m: m,
    }
}

func newHnswVertex(id int64, level int) *hnswVertex {
    vertex := &hnswVertex {
        id: id,
        level: level,
        edges: make([]utils.Set, level + 1),
        edgeMutexes: make([]*sync.RWMutex, level + 1),
    }

    for i := 0; i < (level + 1); i++ {
        vertex.edges[i] = utils.NewSet()
        vertex.edgeMutexes[i] = &sync.RWMutex{}
    }

    return vertex
}

// Vertex
func (v *hnswVertex) addEdge(level int, edge *hnswVertex) {
    defer v.edgeMutexes[level].Unlock()
    v.edgeMutexes[level].Lock()

    v.edges[level].Add(edge)
}

func (v *hnswVertex) getEdges(level int) utils.Set {
    defer v.edgeMutexes[level].RUnlock()
    v.edgeMutexes[level].RLock()

    return v.edges[level]
}

func (v *hnswVertex) setEdges(level int, edges utils.Set) {
    defer v.edgeMutexes[level].Unlock()
    v.edgeMutexes[level].Lock()

    v.edges[level] = edges
}

func (v *hnswVertex) iterateEdges(level int) <-chan *hnswVertex {
    result := make(chan *hnswVertex)
    go func() {
        defer v.edgeMutexes[level].RUnlock()
        v.edgeMutexes[level].RLock()

        for e := range v.edges[level].ToIterator() {
            result <- e.(*hnswVertex)
        }
        close(result)
    }()
    return result
}

// Index
func (index *hnswIndex) Save(writer io.Writer) error {
    return nil
}

func (index *hnswIndex) Load(reader io.Reader) error {
    return nil
}

func (index *hnswIndex) Print() {
    // log.Infof("Len: %d, Max Level: %d", index.Len(), index.maxLevel)

    // visitedIds := utils.NewSet()
    // for i := index.maxLevel; i >= 0; i-- {
    //     log.Infof("Level: %d", i)
    //     log.Infof("Number of entrypoint edges: %d", index.entrypoint.edges[i].Len())

    //     vertexStack := utils.NewStack()
    //     vertexStack.Push(index.entrypoint)
    //     visited := utils.NewSet()
    //     nodeDegrees := make([]int, 0)

    //     for vertexStack.Len() > 0 {
    //         vertex := vertexStack.Pop().(*hnswVertex)
    //         visited.Add(vertex)
    //         visitedIds.Add(vertex.itemId)
    //         nodeDegrees = append(nodeDegrees, vertex.edges[i].Len())

    //         for edge := range vertex.edges[i].ToIterator() {
    //             neighbor := edge.(*hnswVertex)

    //             if !visited.Contains(neighbor) {
    //                 vertexStack.Push(neighbor)
    //             }
    //         }
    //     }

    //     var totalNodeDegree int = 0
    //     for _, nd := range nodeDegrees {
    //         totalNodeDegree += nd
    //     }

    //     log.Infof("Average node degree: %.4f", float64(totalNodeDegree) / float64(len(nodeDegrees)))
    // }

    // log.Infof("Visited nodes: %d", visitedIds.Len())
}

func (index *hnswIndex) Search(ctx context.Context, query math.Vector) SearchResult {
    entrypoint := index.entrypoint
    minDistance := math.EuclideanDistance(query, index.Get(index.entrypoint.id))
    for l := index.maxLevel; l > 0; l-- {
        entrypoint, minDistance = index.greedyClosestNeighbor(query, entrypoint, minDistance, l)
    }

    neighbors := index.searchLevel(query, entrypoint, 100, 0)

    result := make(SearchResult, neighbors.Len())
    for i := 0; i < len(result); i++ {
        item := neighbors.Pop()
        result[i] = &pb.SearchResultItem{Id: item.Value().(*hnswVertex).id, Distance: item.Priority()}
    }

    return result
}

func (index *hnswIndex) Build(ctx context.Context) {
    numThreads := 8
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

    for id, _ := range index.items {
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
    minDistance := math.EuclideanDistance(query, index.Get(index.entrypoint.id))
    for l := maxLevel; l > level; l-- {
        entrypoint, minDistance = index.greedyClosestNeighbor(query, entrypoint, minDistance, l)
    }

    for l := int(math.Min(float32(index.maxLevel), float32(level))); l >= 0; l-- {
        neighbors := index.searchLevel(query, entrypoint, index.efConstruction, l)
        entrypoint = neighbors.Peek().Value().(*hnswVertex)

        for neighbors.Len() > 0 {
            neighbor := neighbors.Pop().Value().(*hnswVertex)

            vertex.addEdge(l, neighbor)
            neighbor.addEdge(l, vertex)
        }
    }

    if level > index.maxLevel {
        index.maxLevel = level
        index.entrypoint = vertex
    }
}

func (index *hnswIndex) randomLevel() int {
    return math.Floor(math.RandomExponential(index.levelMultiplier))
}

func (index *hnswIndex) greedyClosestNeighbor(query math.Vector, entrypoint *hnswVertex, minDistance float32, level int) (*hnswVertex, float32) {
    for {
        var closestNeighbor *hnswVertex
        for neighbor := range entrypoint.iterateEdges(level) {
            if distance := math.EuclideanDistance(query, index.Get(neighbor.id)); distance < minDistance {
                minDistance = distance
                closestNeighbor = neighbor
            }
        }

        if closestNeighbor == nil {
            break
        }

        entrypoint = closestNeighbor
    }

    return entrypoint, minDistance
}

func (index *hnswIndex) searchLevel(query math.Vector, entrypoint *hnswVertex, ef, level int) utils.PriorityQueue {
    entrypointDistance := math.EuclideanDistance(query, index.Get(entrypoint.id))

    candidateVertices := utils.NewMinPriorityQueue(utils.NewPriorityQueueItem(entrypointDistance, entrypoint))
    resultVertices := utils.NewMaxPriorityQueue(utils.NewPriorityQueueItem(entrypointDistance, entrypoint))

    visitedVertices := utils.NewSet(entrypoint)

    for candidateVertices.Len() > 0 {
        candidate := candidateVertices.Pop().Value().(*hnswVertex)
        lowerBound := math.EuclideanDistance(query, index.Get(resultVertices.Peek().Value().(*hnswVertex).id))

        if math.EuclideanDistance(query, index.Get(candidate.id)) > lowerBound {
            break   
        }

        for neighbor := range candidate.iterateEdges(level) {
            if visitedVertices.Contains(neighbor) {
                continue
            }
            visitedVertices.Add(neighbor)

            distance := math.EuclideanDistance(query, index.Get(neighbor.id))
            if (distance < lowerBound) || (resultVertices.Len() < ef) {
                candidateVertices.Push(utils.NewPriorityQueueItem(distance, neighbor))
                resultVertices.Push(utils.NewPriorityQueueItem(distance, neighbor))

                if resultVertices.Len() > ef {
                    resultVertices.Pop()
                }
            }
        }
    }

    return resultVertices.Reverse()
}
