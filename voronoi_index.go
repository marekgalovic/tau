// Implementation of Parallel K-Means++
package tau

import (
    "sort";
    "sync";
    "runtime";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/utils";
)

type voronoiIndex struct {
    baseIndex
    itemIds []int
    cost math.Float
    centroids []math.Vector
    centroidAssignments [][]int
}

type itemCentroidDistance struct {
    itemId int
    centroidId int
    distance math.Float
}

func VoronoiIndex(size int, metric string) Index {
    return &voronoiIndex {
        baseIndex: newBaseIndex(size, metric),
        centroids: make([]math.Vector, 0),
    }
}

func (index *voronoiIndex) Add(id int, value math.Vector) error {
    if err := index.baseIndex.Add(id, value); err != nil {
        return err
    }

    index.itemIds = append(index.itemIds, id)
    return nil
}

func (index *voronoiIndex) Build() {
    index.initializeCentroids()
    index.lloyd()
}

func (index *voronoiIndex) Search(query math.Vector) SearchResult {
    centroidId, _ := index.closestCentroid(query)
    ids := utils.NewSet()
    for _, id := range index.centroidAssignments[centroidId] {
        ids.Add(id)
    }

    result := newSearchResult(index, query, ids)
    sort.Sort(result)

    return result
}

func (index *voronoiIndex) numCentroids() int {
    return int(index.Len() / 1000)
}

func (index *voronoiIndex) initialCentroid() math.Vector {
    var result math.Vector
    for _, vec := range index.items {
        result = vec
        break
    }
    return result
}

func (index *voronoiIndex) initializeCentroids() {
    // K-means++ centroid initialization
    index.centroids = append(index.centroids, index.initialCentroid())

    distances := make(math.Vector, len(index.items))
    distanceIds := make([]int, len(index.items))
    for len(index.centroids) < index.numCentroids() {
        i := 0
        var total math.Float
        for icDistance := range index.itemCentroidDistances() {
            distances[i] = icDistance.distance
            distanceIds[i] = icDistance.itemId
            total += icDistance.distance
            i++
        }

        i = 0
        target := math.RandomUniform() * total
        for total = distances[0]; total < target; total += distances[i] {
            i++
        }
        
        index.centroids = append(index.centroids, index.items[distanceIds[i]])
    }
}

func (index *voronoiIndex) lloyd() {
    var previousCost math.Float
    for i := 0; i < 100; i++ {
        index.centroidAssignments = make([][]int, index.numCentroids())
        newCentroids := make([]math.Vector, index.numCentroids())
        index.cost = 0

        for icDistance := range index.itemCentroidDistances() {
            if len(newCentroids[icDistance.centroidId]) == 0 {
                newCentroids[icDistance.centroidId] = make(math.Vector, index.size)
            }

            index.cost += icDistance.distance
            newCentroids[icDistance.centroidId] = math.VectorAdd(newCentroids[icDistance.centroidId], index.items[icDistance.itemId])
            index.centroidAssignments[icDistance.centroidId] = append(index.centroidAssignments[icDistance.centroidId], icDistance.itemId)
        }

        for centroidId, centroid := range newCentroids {
            index.centroids[centroidId] = math.VectorScalarDivide(centroid, math.Float(len(index.centroidAssignments[centroidId])))
        }

        if math.Abs(index.cost - previousCost) < 10 {
            break
        }
        previousCost = index.cost
    }
}

func (index *voronoiIndex) closestCentroid(item math.Vector) (int, math.Float) {
    minDistance := math.MaxFloat
    var id int
    for centroidId, centroid := range index.centroids {
        if itemCentroidDistance := math.EuclideanDistance(item, centroid); itemCentroidDistance < minDistance {
            minDistance = itemCentroidDistance
            id = centroidId
        }
    }  
    return id, minDistance
}

func (index *voronoiIndex) itemCentroidDistances() <-chan *itemCentroidDistance {
    wg := &sync.WaitGroup{}
    results := make(chan *itemCentroidDistance)
    numThreads := runtime.NumCPU()
    sliceSize := int(index.Len() / numThreads)

    for t := 0; t < numThreads; t++ {
        lb := t * sliceSize
        ub := lb + sliceSize
        if t == numThreads - 1 {
            ub += index.Len() - (numThreads * sliceSize)
        }

        wg.Add(1)
        go func(lb, ub int) {
            defer wg.Done()
            for _, itemId := range index.itemIds[lb:ub] {
                centroidId, distance := index.closestCentroid(index.items[itemId])
                results <- &itemCentroidDistance{itemId, centroidId, distance}
            }
        }(lb, ub)
    }

    go func() {
        wg.Wait()
        close(results)
    }()

    return results
}
