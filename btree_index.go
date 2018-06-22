// Binary search tree
package tau

import (
    "fmt";
    "sort";
    "time";
    "sync";
    "context";
    "strings";
    goMath "math";

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
    value math.Vector
    itemIds []int
    leftNode *btreeNode
    rightNode *btreeNode
}

type btreeSplitArgs struct {
    parent *btreeNode
    leftIds []int
    rightIds []int
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

func (index *btreeIndex) Search(query math.Vector) SearchResult {
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

func (index *btreeIndex) csValue(value []float64) string {
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

func (index *btreeIndex) searchTree(tree *btreeNode, query math.Vector, ctx context.Context, results chan []int, done chan struct{}) {
    distanceThreshold := goMath.Log(math.Length(query)) / 10

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
        if goMath.Abs(distance) <= distanceThreshold {
            nodes.Push(node.leftNode)
            nodes.Push(node.rightNode)
        } else 
        if distance <= 0 {
            nodes.Push(node.leftNode)
        } else {
            nodes.Push(node.rightNode)
        }
    }

    done <- struct{}{}
}

func newBtree(index *btreeIndex) *btreeNode {
    // Worst O(n) to sample initial points
    pointIds := sampleDistinctInts(2, len(index.items))
    var pointA, pointB math.Vector
    for _, point := range index.items {
        if pointIds[0] == 0 {
            pointA = point
        }
        if pointIds[1] == 0 {
            pointB = point
        }
        if (pointIds[0] <= 0) && (pointIds[1] <= 0) {
            break
        }
        pointIds[0]--
        pointIds[1]--
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

    root := &btreeNode {value: split}

    stack := utils.NewThreadSafeStack()
    stack.Push(&btreeSplitArgs{root, leftIds, rightIds})

    for stack.Len() > 0 {
        wg := &sync.WaitGroup{}
        args := stack.Pop().(*btreeSplitArgs)

        var leftIds, rightIds []int
        if len(args.leftIds) > index.maxLeafItems {
            wg.Add(1)
            go func() {
                defer wg.Done()
                args.parent.leftNode, leftIds, rightIds = splitSamples(index, args.leftIds)
                stack.Push(&btreeSplitArgs{args.parent.leftNode, leftIds, rightIds})
            }()
        } else {
            args.parent.leftNode = &btreeNode{itemIds: args.leftIds}
        }

        if len(args.rightIds) > index.maxLeafItems {
            wg.Add(1)
            go func() {
                defer wg.Done()
                args.parent.rightNode, leftIds, rightIds = splitSamples(index, args.rightIds)
                stack.Push(&btreeSplitArgs{args.parent.rightNode, leftIds, rightIds})
            }()
        } else {
            args.parent.rightNode = &btreeNode{itemIds: args.rightIds}
        }
        
        wg.Wait()
    }

    return root
}

func splitSamples(index *btreeIndex, ids []int) (*btreeNode, []int, []int) {
    pointIds := sampleDistinctInts(2, len(ids))
    pointA := index.items[ids[pointIds[0]]]
    pointB := index.items[ids[pointIds[1]]]
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

    return &btreeNode{value: split}, leftIds, rightIds
}

func (bt *btreeNode) isLeaf() bool {
    return (bt.leftNode == nil) && (bt.rightNode == nil)
}
