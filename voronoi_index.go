// Implementation of Parallel K-Means++
package tau

import (
    "sort";
    "sync";
    "runtime";
    "math/rand";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/utils";
)

type voronoiIndex struct {
    baseIndex
    root *voronoiCell
}

type voronoiCell struct {
    centroid math.Vector
    cost math.Float
    itemIds []int
    children []*voronoiCell
}

type itemCellDistance struct {
    itemId int
    cellId int
    distance math.Float
}

func VoronoiIndex(size int, metric string) Index {
    return &voronoiIndex {
        baseIndex: newBaseIndex(size, metric),
        root: &voronoiCell{},
    }
}

func (index *voronoiIndex) Add(id int, value math.Vector) error {
    if err := index.baseIndex.Add(id, value); err != nil {
        return err
    }

    index.root.itemIds = append(index.root.itemIds, id)
    return nil
}

func (index *voronoiIndex) Build() {
    initialCells := index.initializeCells(index.root, index.numCentroids())
    index.root.children = index.kMeans(index.root, initialCells)
}

func (index *voronoiIndex) Search(query math.Vector) SearchResult {
    cellId, _ := index.closestCell(query, index.root.children)
    ids := utils.NewSet()
    for _, id := range index.root.children[cellId].itemIds {
        ids.Add(id)
    }

    result := newSearchResult(index, query, ids)
    sort.Sort(result)

    return result
    return nil
}

func (index *voronoiIndex) numCentroids() int {
    return int(math.Min(math.Float(index.Len() / 1000), 100))
}

func (index *voronoiIndex) initialCell(ids []int) *voronoiCell {
    itemId := ids[rand.Intn(len(ids))]

    return &voronoiCell{centroid: index.items[itemId]}
}

func (index *voronoiIndex) initializeCells(parent *voronoiCell, k int) []*voronoiCell {
    // K-means++ centroid initialization
    cells := []*voronoiCell {
        index.initialCell(parent.itemIds),
    }

    distances := make([]*itemCellDistance, len(parent.itemIds))
    for i := 0; i < k; i++ {
        j := 0
        var cumulativeDistance math.Float
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
    }
    
    return cells
}

func (index *voronoiIndex) kMeans(parent *voronoiCell, cells []*voronoiCell) []*voronoiCell {
    // Lloyd's iteration
    newCentroids := make([]math.Vector, len(cells))

    var previousCost math.Float
    for i := 0; i < 100; i++ {
        for j, _ := range cells {
            cells[j].itemIds = make([]int, 0)
            newCentroids[j] = make(math.Vector, index.size)
        }

        var cost math.Float
        for icDistance := range index.itemCellDistances(parent.itemIds, cells) {
            cells[icDistance.cellId].cost += icDistance.distance
            cells[icDistance.cellId].itemIds = append(cells[icDistance.cellId].itemIds, icDistance.itemId)
            newCentroids[icDistance.cellId] = math.VectorAdd(newCentroids[icDistance.cellId], index.Get(icDistance.itemId))

            cost += icDistance.distance
        }

        for cellId, centroid := range newCentroids {
            cells[cellId].centroid = math.VectorScalarDivide(centroid, math.Float(len(cells[cellId].itemIds)))
        }

        if math.Abs(cost - previousCost) < 10 {
            break
        }
        previousCost = cost
    }

    return cells
}

func (index *voronoiIndex) closestCell(item math.Vector, cells []*voronoiCell) (int, math.Float) {
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

func (index *voronoiIndex) itemCellDistances(ids []int, cells []*voronoiCell) <-chan *itemCellDistance {
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
                cellId, distance := index.closestCell(index.items[itemId], cells)
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
