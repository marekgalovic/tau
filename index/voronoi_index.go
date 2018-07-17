// Implementation of Parallel K-Means++
 package index

import (
    "fmt";
    "io";
    "sort";
    "sync";
    "runtime";
    "math/rand";
    "context";
    "encoding/binary";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/utils";
    pb "github.com/marekgalovic/tau/protobuf";
)

type voronoiIndex struct {
    baseIndex
    splitFactor int
    maxCellItems int
    root *voronoiCell
}

type voronoiCell struct {
    centroid math.Vector
    cost float32
    itemIds []int64
    children []*voronoiCell
}

type itemCellDistance struct {
    itemId int64
    cellId int
    distance float32
}

// Voronoi index builds a tree using kMeans algorithm.
// Every node of this tree (except leaf nodes) has number of children determined by
// splitFactor argument.
// Once there is <= maxCellItems in a node, it's considered to be a leaf node.
func NewVoronoiIndex(size int, metric string, splitFactor, maxCellItems int) Index {
    return &voronoiIndex {
        baseIndex: newBaseIndex(size, metric),
        splitFactor: splitFactor,
        maxCellItems: maxCellItems,
        root: &voronoiCell{},
    }
}

func (index *voronoiIndex) Reset() {
    index.root = &voronoiCell{}
    index.baseIndex.Reset()
}

func (index *voronoiIndex) Add(id int64, value math.Vector) error {
    if err := index.baseIndex.Add(id, value); err != nil {
        return err
    }

    index.root.itemIds = append(index.root.itemIds, id)
    return nil
}

func (index *voronoiIndex) ToProto() *pb.Index {
    proto := index.baseIndex.ToProto()
    proto.Options = &pb.Index_Voronoi {
        Voronoi: &pb.VoronoiIndexOptions {
            SplitFactor: int32(index.splitFactor),
            MaxCellItems: int32(index.maxCellItems),
        },
    }
    return proto
}

// Build builds the search tree.
func (index *voronoiIndex) Build(ctx context.Context) {
    stack := utils.NewStack()
    stack.Push(index.root)

    for stack.Len() > 0 {
        parent := stack.Pop().(*voronoiCell)

        if len(parent.itemIds) <= index.maxCellItems {
            continue
        }

        initialCells := index.initializeCells(ctx, parent, index.splitFactor)
        parent.children = index.kMeans(ctx, parent, initialCells)
        parent.itemIds = nil

        for _, child := range parent.children {
            stack.Push(child)
        }

        select {
        case <-ctx.Done():
            return
        default:
        }
    }
}

func (index *voronoiIndex) Save(writer io.Writer) error {
    if err := index.writeHeader(writer, []byte("tauVOR")); err != nil {
        return err
    }

    // Index specific headers
    if err := binary.Write(writer, binary.LittleEndian, int32(index.splitFactor)); err != nil {
        return err
    }
    if err := binary.Write(writer, binary.LittleEndian, int32(index.maxCellItems)); err != nil {
        return err
    }

    return index.writeCells(writer)
}

func (index *voronoiIndex) writeCells(writer io.Writer) error {
    stack := utils.NewStack()
    stack.Push(index.root)

    for stack.Len() > 0 {
        cell := stack.Pop().(*voronoiCell)

        if err := binary.Write(writer, binary.LittleEndian, cell.isLeaf()); err != nil {
            return err
        }
        if err := binary.Write(writer, binary.LittleEndian, cell.hasCentroid()); err != nil {
            return err
        }
        if cell.hasCentroid() {
            if err := binary.Write(writer, binary.LittleEndian, cell.centroid); err != nil {
                return err
            }
        }

        if cell.isLeaf() {
            if err := binary.Write(writer, binary.LittleEndian, int32(len(cell.itemIds))); err != nil {
                return err
            }
            if err := binary.Write(writer, binary.LittleEndian, cell.itemIds); err != nil {
                return err
            }
        } else {
            for i := 0; i < index.splitFactor; i++ {
                stack.Push(cell.children[index.splitFactor - 1 - i])
            }
        }
    }
    return nil
}

func (index *voronoiIndex) Load(reader io.Reader) error {
    indexType, err := index.readHeader(reader)
    if err != nil {
        return err
    }
    if string(indexType) != "tauVOR" {
        return fmt.Errorf("Invalid index type `%s`", indexType)
    }

    var splitFactor int32
    if err := binary.Read(reader, binary.LittleEndian, &splitFactor); err != nil {
        return err
    }
    index.splitFactor = int(splitFactor)

    var maxCellItems int32
    if err := binary.Read(reader, binary.LittleEndian, &maxCellItems); err != nil {
        return err
    }
    index.maxCellItems = int(maxCellItems)
    
    return index.readCells(reader)
}

func (index *voronoiIndex) readCells(reader io.Reader) error {
    root := &voronoiCell{}

    stack := utils.NewStack()
    stack.Push(root)

    for stack.Len() > 0 {
        cell := stack.Pop().(*voronoiCell)

        var isLeaf, hasCentroid bool
        if err := binary.Read(reader, binary.LittleEndian, &isLeaf); err != nil {
            return err
        }
        if err := binary.Read(reader, binary.LittleEndian, &hasCentroid); err != nil {
            return err
        }
        if hasCentroid {
            cell.centroid = make(math.Vector, index.size)
            if err := binary.Read(reader, binary.LittleEndian, &cell.centroid); err != nil {
                return err
            }
        }

        if isLeaf {
            var itemsCount int32
            if err := binary.Read(reader, binary.LittleEndian, &itemsCount); err != nil {
                return err
            }
            cell.itemIds = make([]int64, itemsCount)
            if err := binary.Read(reader, binary.LittleEndian, &cell.itemIds); err != nil {
                return err
            }
        } else {
            cell.children = make([]*voronoiCell, index.splitFactor)
            for i := 0; i < index.splitFactor; i++ {
                cell.children[index.splitFactor - 1 - i] = &voronoiCell{}
                stack.Push(cell.children[index.splitFactor - 1 - i])
            }
        }
    }
    return nil
}

// Search traverses the built tree to find nearest points given a query.
// If a difference of distance between nearest cell and other cells in a given
// node is < 10% then other nodes are also considered with lower priority.
// Traversal stops once there are int(splitFactor / 5) * maxCellItems candidates.
func (index *voronoiIndex) Search(ctx context.Context, query math.Vector) SearchResult {
    kNearest := int(index.splitFactor / 5)
    maxResultCandidates := kNearest * index.maxCellItems

    stack := utils.NewStack()
    stack.Push(index.root)

    resultIds := utils.NewSet()
    searchLoop:
    for stack.Len() > 0 {
        select {
        case <-ctx.Done():
            return nil
        default:
        }
        cell := stack.Pop().(*voronoiCell)

        if cell.isLeaf() {
            for _, itemId := range cell.itemIds {
                resultIds.Add(itemId)

                if resultIds.Len() >= maxResultCandidates {
                    break searchLoop
                }
            }
            continue
        }

        nearCells := index.kNearestCells(kNearest, query, cell.children)
        nearestCell := nearCells[0]

        for i := 0; i < len(nearCells) - 1; i++ {
            candidate := nearCells[len(nearCells) - 1 - i]
            if ((candidate.distance - nearestCell.distance) / nearestCell.distance) < 1e-1 {
                stack.Push(cell.children[candidate.cellId])
            }
        }
        stack.Push(cell.children[nearestCell.cellId])
    }

    return newSearchResult(index, query, resultIds)
}

func (index *voronoiIndex) initialCell(ids []int64) *voronoiCell {
    itemId := ids[rand.Intn(len(ids))]

    return &voronoiCell{centroid: index.items[itemId]}
}

func (index *voronoiIndex) initializeCells(ctx context.Context, parent *voronoiCell, k int) []*voronoiCell {
    // K-means++ centroid initialization
    cells := []*voronoiCell {
        index.initialCell(parent.itemIds),
    }

    distances := make([]*itemCellDistance, len(parent.itemIds))
    for i := 0; i < k; i++ {
        j := 0
        var cumulativeDistance float32
        for icDistance := range index.itemCellDistances(parent.itemIds, cells) {
            distances[j] = icDistance
            cumulativeDistance += icDistance.distance
            j++
        }

        j = 0
        target := math.RandomUniform() * cumulativeDistance
        for sum := distances[0].distance; sum < target; sum += distances[j].distance {
            j++
        }
        cells = append(cells, &voronoiCell{centroid: index.Get(distances[j].itemId)})

        select {
        case <-ctx.Done():
            return nil
        default:
        }
    }
    
    return cells
}

func (index *voronoiIndex) kMeans(ctx context.Context, parent *voronoiCell, cells []*voronoiCell) []*voronoiCell {
    // Lloyd's iteration
    newCentroids := make([]math.Vector, len(cells))

    var previousCost float32
    for i := 0; i < 100; i++ {
        for j, _ := range cells {
            cells[j].itemIds = make([]int64, 0)
            newCentroids[j] = make(math.Vector, index.size)
        }

        var cost float32
        for icDistance := range index.itemCellDistances(parent.itemIds, cells) {
            cells[icDistance.cellId].cost += icDistance.distance
            cells[icDistance.cellId].itemIds = append(cells[icDistance.cellId].itemIds, icDistance.itemId)
            newCentroids[icDistance.cellId] = math.VectorAdd(newCentroids[icDistance.cellId], index.Get(icDistance.itemId))

            cost += icDistance.distance
        }

        for cellId, centroid := range newCentroids {
            cells[cellId].centroid = math.VectorScalarDivide(centroid, float32(len(cells[cellId].itemIds)))
        }

        if math.Abs(cost - previousCost) < 10 {
            break
        }
        previousCost = cost

        select {
        case <-ctx.Done():
            return nil
        default:
        }
    }

    return cells
}

func (index *voronoiIndex) nearestCell(item math.Vector, cells []*voronoiCell) (int, float32) {
    minDistance := math.MaxFloat
    var id int
    for cellId, cell := range cells {
        if distance := math.EuclideanDistance(item, cell.centroid); distance < minDistance {
            minDistance = distance
            id = cellId
        }
    }
    return id, minDistance
}

func (index *voronoiIndex) kNearestCells(k int, item math.Vector, cells []*voronoiCell) []*itemCellDistance {
    result := make([]*itemCellDistance, len(cells))
    for cellId, cell := range cells {
        result[cellId] = &itemCellDistance{0, cellId, math.EuclideanDistance(item, cell.centroid)}
    }

    sort.Slice(result, func(i, j int) bool {
        return result[i].distance < result[j].distance
    })

    if len(result) > k {
        return result[:k]
    }
    return result
}

func (index *voronoiIndex) itemCellDistances(ids []int64, cells []*voronoiCell) <-chan *itemCellDistance {
    wg := &sync.WaitGroup{}
    results := make(chan *itemCellDistance)
    numThreads := runtime.NumCPU()
    sliceSize := int(len(ids) / numThreads)

    for t := 0; t < numThreads; t++ {
        lb := t * sliceSize
        ub := lb + sliceSize
        if t == numThreads - 1 {
            ub += len(ids) - (numThreads * sliceSize)
        }

        wg.Add(1)
        go func(lb, ub int) {
            defer wg.Done()
            for _, itemId := range ids[lb:ub] {
                cellId, distance := index.nearestCell(index.items[itemId], cells)
                results <- &itemCellDistance{itemId, cellId, distance}
            }
        }(lb, ub)
    }

    go func() {
        wg.Wait()
        close(results)
    }()

    return results
}

func (cell *voronoiCell) isLeaf() bool {
    return len(cell.children) == 0
}

func (cell *voronoiCell) hasCentroid() bool {
    return len(cell.centroid) > 0
}
