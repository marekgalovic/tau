// Binary search tree
package tau

import (
    "fmt";
    "sort";
    "time";
    "context";
    "strings";
    // goMath "math";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/utils";
)

type btreeIndex struct {
    baseIndex
    numTrees int
    maxLeafItems int
    trees []*btreeNode
}

type btreeNode struct {
    value []float32
    itemIds []int
    leftNode *btreeNode
    rightNode *btreeNode
}

func NewBtreeIndex(size int, metric string, numTrees, maxLeafItems int) Index {
    if numTrees < 1 {
        panic("Num trees has to be >= 1")
    }

    return &btreeIndex {
        baseIndex: newBaseIndex(size, metric),
        numTrees: numTrees,
        maxLeafItems: maxLeafItems,
        trees: make([]*btreeNode, numTrees),
    }
}

func (index *btreeIndex) Build() {
    treeChans := make([]chan *btreeNode, index.numTrees)

    for t := 0; t < index.numTrees; t++ {
        treeChans[t] = make(chan *btreeNode)
        go func(treeChan chan *btreeNode) {
            treeChan <- newBtree(index)
        }(treeChans[t])
    }

    for i, treeChan := range treeChans {
        index.trees[i] = <- treeChan
        close(treeChan)
    }
}

func (index *btreeIndex) Search(query []float32) SearchResult {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    resultsChan := make(chan []int)
    doneChan := make(chan struct{})
    for _, tree := range index.trees {
        go index.searchTree(tree, query, ctx, resultsChan, doneChan) 
    }

    var nDone int
    resultIds := utils.NewSet()

    resultsLoop:
    for {
        select {
        case resultSlice := <- resultsChan:
            for _, id := range resultSlice {
                resultIds.Add(id)
            }
        case <- doneChan:
            nDone++
            if nDone == len(index.trees) {
                break resultsLoop
            }
        case <- time.After(1 * time.Second):
            break resultsLoop
        }
    }

    result := newSearchResult(index, query, resultIds)
    sort.Sort(result)

    return result
}

func (index *btreeIndex) csValue(value []float32) string {
    stringValue := make([]string, len(value))
    for i, v := range value {
        stringValue[i] = fmt.Sprintf("%.4f", v)
    }
    return strings.Join(stringValue, ",")
}

func (index *btreeIndex) printTree(tree *btreeNode, level int) {
    tabs := strings.Repeat("\t", level)
    fmt.Println(tabs + index.csValue(tree.value))
    if !tree.isLeaf() {
        index.printTree(tree.leftNode, level + 1)
        index.printTree(tree.rightNode, level + 1)
    }
}

func (index *btreeIndex) Save(path string) error {
    for treeIdx, tree := range index.trees {
        fmt.Println("Tree:", treeIdx)
        index.printTree(tree, 0)
    }
    return nil
}

func (index *btreeIndex) searchTree(tree *btreeNode, query []float32, ctx context.Context, results chan []int, done chan struct{}) {
    distanceThreshold := math.Sqrt(math.VectorLength(query))

    nodes := utils.NewStack()
    nodes.Push(tree)

    for nodes.Len() > 0 {
        select {
        case <- ctx.Done():
            return
        default:
        }

        node := nodes.Pop().(*btreeNode)
        if node.isLeaf() {
            results <- node.itemIds
            continue
        }

        distance := math.PointPlaneDistance(query, node.value)

        if math.Abs(distance) <= distanceThreshold {
            nodes.Push(node.leftNode)
            nodes.Push(node.rightNode)
        } else if distance <= 0 {
            nodes.Push(node.leftNode)
        } else {
            nodes.Push(node.rightNode)
        }
    }

    done <- struct{}{}
}

func newBtree(index *btreeIndex) *btreeNode {
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
    split := math.EquidistantPlane(pointA, pointB)

    leftIds := make([]int, 0)
    rightIds := make([]int, 0)
    for idx, item := range index.items {
        if math.PointPlaneDistance(item, split) <= 0 {
            leftIds = append(leftIds, idx)
        } else {
            rightIds = append(rightIds, idx)
        }
    }

    return &btreeNode {
        value: split,
        leftNode: newBtreeChild(index, leftIds),
        rightNode: newBtreeChild(index, rightIds),
    }
}

func newBtreeChild(index *btreeIndex, ids []int) *btreeNode {
    if len(ids) <= index.maxLeafItems {
        return &btreeNode {
            itemIds: ids,
        }
    }

    // Constant time sample
    aIdx, bIdx := sampleDistinctInts(len(ids))
    pointA := index.items[ids[aIdx]]
    pointB := index.items[ids[bIdx]]
    split := math.EquidistantPlane(pointA, pointB)

    leftIds := make([]int, 0)
    rightIds := make([]int, 0)
    for idx := range ids {
        item := index.items[idx]
        if math.PointPlaneDistance(item, split) <= 0 {
            leftIds = append(leftIds, idx)
        } else {
            rightIds = append(rightIds, idx)
        }
    }

    return &btreeNode {
        value: split,
        leftNode: newBtreeChild(index, leftIds),
        rightNode: newBtreeChild(index, rightIds),  
    }
}

func (bt *btreeNode) isLeaf() bool {
    return (bt.leftNode == nil) && (bt.rightNode == nil)
}
