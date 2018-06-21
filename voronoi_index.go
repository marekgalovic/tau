// Implementation of Parallel K-Means++
package tau

import (
    "fmt";
    
    "github.com/marekgalovic/tau/math";
)

type voronoiIndex struct {
    baseIndex
    l int
    centroids [][]float32
}

func VoronoiIndex(size int, metric string, l int) Index {
    return &voronoiIndex{
        baseIndex: newBaseIndex(size, metric),
        l: l,
        centroids: make([][]float32, 0),
    }
}

func (i *voronoiIndex) Build() {
    // Dummy k-means
    fmt.Println("Build")
    i.initCentroids()
    i.kMeans()
}

func (index *voronoiIndex) Save(f string) error {
    return nil
}

func (i *voronoiIndex) initCentroids() {
    i.centroids = append(i.centroids, i.randomItem())

    for j := 0; j < i.l - 1; j++ {
        maxDistance := float32(-1.0)
        var newCentroidId int

        for iId, item := range i.items {
            var totalItemDistance float32
            for _, centroid := range i.centroids {
                totalItemDistance += math.EuclideanDistance(item, centroid)
            }
            if totalItemDistance > maxDistance {
                maxDistance = totalItemDistance
                newCentroidId = iId
            }
        }
        i.centroids = append(i.centroids, i.items[newCentroidId])
    }
}

func (i *voronoiIndex) kMeans() {
    var previousTotalCost float32
    for k := 0; k < 10000; k++ {
        newCentroids := make(map[int][]float32)
        newCentroidItemCounts := make(map[int]float32)

        var totalCost float32
        for _, item := range i.items {
            minDistance := float32(math.MaxFloat32)
            var assignedCentroid int

            for cid, centroid := range i.centroids {
                distance := math.EuclideanDistance(item, centroid)
                if distance < minDistance {
                    minDistance = distance
                    assignedCentroid = cid
                }
            }

            totalCost += minDistance

            if _, exists := newCentroids[assignedCentroid]; !exists {
                newCentroids[assignedCentroid] = make([]float32, i.size)
            }
            newCentroidItemCounts[assignedCentroid] += 1

            for component_idx := 0; component_idx < i.size; component_idx++ {
                newCentroids[assignedCentroid][component_idx] = newCentroids[assignedCentroid][component_idx] * ((newCentroidItemCounts[assignedCentroid] - 1) / newCentroidItemCounts[assignedCentroid]) + (item[component_idx] / newCentroidItemCounts[assignedCentroid])
            }
        }

        for cid, newCentroid := range newCentroids {
            i.centroids[cid] = newCentroid
        }

        fmt.Println(k, totalCost)
        if math.Abs(totalCost - previousTotalCost) < 1e-1 {
            fmt.Println("Converged")
            break
        }
        previousTotalCost = totalCost
    }
}

func (i *voronoiIndex) Search(query []float32) SearchResult {
    return nil
}

func(i *voronoiIndex) numCentroids() int {
    return math.Trunc(math.Sqrt(float32(len(i.items))))
}
