// Implementation of K-Means||
package tau

import (
    "fmt";
    "math"
)

type voronoiIndex struct {
    baseIndex
    l int
    centroids [][]float32
}

func VoronoiIndex(size int, l int) Index {
    return &voronoiIndex{
        baseIndex: newBaseIndex(size),
        l: l,
        centroids: make([][]float32, 0),
    }
}

func (i *voronoiIndex) Build() {
    // Dummy k-means
    fmt.Println("Build")

    for cid := 0; cid < i.l; cid++ {
        i.centroids = append(i.centroids, randomVector(i.size))
    }

    var previousTotalCost float32
    for k := 0; k < 10000; k++ {
        newCentroids := make(map[int][]float32)
        newCentroidItemCounts := make(map[int]float32)

        var totalCost float32
        for _, item := range i.items {
            minDistance := float32(math.MaxFloat32)
            var assignedCentroid int

            for cid, centroid := range i.centroids {
                distance := euclideanDistance(item, centroid)
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
                newCentroids[assignedCentroid][component_idx] = newCentroids[assignedCentroid][component_idx] * (newCentroidItemCounts[assignedCentroid] - 1) / newCentroidItemCounts[assignedCentroid] + item[component_idx] / newCentroidItemCounts[assignedCentroid]
            }
        }

        for cid, newCentroid := range newCentroids {
            i.centroids[cid] = newCentroid
        }

        fmt.Println(k, totalCost)
        if math.Abs(float64(totalCost - previousTotalCost)) < 1e-1 {
            fmt.Println("Converged")
            break
        }
        previousTotalCost = totalCost
    }
}

func (i *voronoiIndex) Search() {

}

func(i *voronoiIndex) numCentroids() int {
    return int(math.Trunc(math.Sqrt(float64(len(i.items)))))  
}
