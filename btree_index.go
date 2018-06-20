// Binary search tree
package tau

import (
    "github.com/marekgalovic/tau/math"
)

type btreeIndex struct {
    baseIndex
    numTrees int
    maxLeafNodes int
    trees []*btree
}

type btree struct {
    value []float32
    itemIds []int
    leftNode *btree
    rightNode *btree
}

func NewBtreeIndex(size, numTrees, maxLeafNodes int) Index {
    if numTrees < 1 {
        panic("Num trees has to be >= 1")
    }

    return &btreeIndex {
        baseIndex: newBaseIndex(size),
        numTrees: numTrees,
        maxLeafNodes: maxLeafNodes,
        trees: make([]*btree, numTrees),
    }
}

func (index *btreeIndex) Build() {
    treeChans := make([]chan *btree, index.numTrees)

    for t := 0; t < index.numTrees; t++ {
        treeChans[t] = make(chan *btree)
        go func(treeChan chan *btree) {
            treeChan <- newBtree(index, []int{}, 0)
        }(treeChans[t])
    }

    for i, treeChan := range treeChans {
        index.trees[i] = <- treeChan
        close(treeChan)
    }
}

func (i *btreeIndex) Search(query []float32) (map[int]float32, error) {
    return nil, nil
}

func newBtree(index *btreeIndex, ids []int, dim int) *btree {
    // Worst O(n) to sample initial points
    aIdx, bIdx := sampleDistinctInts(len(index.items))
    var pointA, pointB []float32
    for _, point := range index.items {
        if aIdx == 0 {
            pointA = point
        }
        if bIdx == 0 {
            pointB = point
        }
        if (aIdx <= 0) && (bIdx <= 0) {
            break
        }
        aIdx--
        bIdx--
    }
    split := pointA[0] + (pointB[0] - pointA[0]) / 2

    leftIds := make([]int, 0)
    rightIds := make([]int, 0)
    leftValue := make([]float32, index.size)
    rightValue := make([]float32, index.size)
    for idx, item := range index.items {
        if item[0] <= split {
            leftIds = append(leftIds, idx)
            leftValue = math.VectorAdd(leftValue, item)
        } else {
            rightIds = append(rightIds, idx)
            rightValue = math.VectorAdd(rightValue, item)
        }
    }

    // Average node vectors
    leftValue = math.VectorScalarDivide(leftValue, float32(len(leftIds)))
    rightValue = math.VectorScalarDivide(rightValue, float32(len(rightIds)))

    return &btree {
        leftNode: newBtreeChild(index, leftValue, leftIds, 1),
        rightNode: newBtreeChild(index, rightValue, rightIds, 1),
    }
}

func newBtreeChild(index *btreeIndex, value []float32, ids []int, dim int) *btree {
    if len(ids) <= 100 {
        return &btree {
            value: value,
            itemIds: ids,
        }
    }

    // Constant time sample
    aIdx, bIdx := sampleDistinctInts(len(ids))
    pointA := index.items[ids[aIdx]]
    pointB := index.items[ids[bIdx]]

    if dim >= index.size {
        dim = 0
    }
    split := pointA[dim] + (pointB[dim] - pointA[dim]) / 2

    leftIds := make([]int, 0)
    rightIds := make([]int, 0)
    leftValue := make([]float32, index.size)
    rightValue := make([]float32, index.size)
    for idx := range ids {
        item := index.items[idx]
        if item[dim] <= split {
            leftIds = append(leftIds, idx)
            leftValue = math.VectorAdd(leftValue, item)
        } else {
            rightIds = append(rightIds, idx)
            rightValue = math.VectorAdd(rightValue, item)
        }
    }

    // Average node vectors
    leftValue = math.VectorScalarDivide(leftValue, float32(len(leftIds)))
    rightValue = math.VectorScalarDivide(rightValue, float32(len(rightIds)))

    return &btree {
        value: value,
        leftNode: newBtreeChild(index, leftValue, leftIds, dim + 1),
        rightNode: newBtreeChild(index, rightValue, rightIds, dim + 1),  
    }
}
