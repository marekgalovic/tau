// Binary search tree
package tau

import (
    "sort";
    "time";
    "sync";
    "context";

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

// bTree index builds a forest of randomized binary search trees.
// Every node uniformly samples a pair of points and using a hyperplane equidistant
// to these two points splits space based on signed point plane distance.
// Once there is <= maxLeafItems in a node, it's considered to be a leaf node.
func NewBtreeIndex(size int, metric string, numTrees, maxLeafItems int) Index {
    if numTrees < 1 {
        panic("Num trees has to be >= 1")
    }

    return &btreeIndex {
        baseIndex: newBaseIndex(size, metric),
        numTrees: numTrees,
        maxLeafItems: maxLeafItems,
    }
}

// Build builds the forest.
// As individual trees are independent of each other they
// are built in parallel.
func (index *btreeIndex) Build() {
    index.trees = make([]*btreeNode, index.numTrees)
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

// Search traverses trees in the build forest in parallel.
// If query lies on one side of the plane but condition abs(distance) <= log(l2_length(query)) / 10
// is true then other side of the plane is also considered with lower priority.
// Traversal stops once there are numTrees * maxLeafItems candidates. 
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

    searchLoop:
    for {
        select {
        case resultSlice := <- resultsChan:
            for _, id := range resultSlice {
                resultIds.Add(id)
                if resultIds.Len() == index.numTrees * index.maxLeafItems {
                    break searchLoop
                }
            }
        case <- doneChan:
            nDone++
            if nDone == len(index.trees) {
                break searchLoop
            }
        case <- time.After(1 * time.Second):
            break searchLoop
        }
    }

    result := newSearchResult(index, query, resultIds)
    sort.Sort(result)

    return result
}

func (index *btreeIndex) searchTree(tree *btreeNode, query math.Vector, ctx context.Context, results chan []int, done chan struct{}) {
    distanceThreshold := math.Log(math.Length(query)) / 10

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
        pushBoth := math.Abs(distance) <= distanceThreshold
        if distance <= 0 {
            if pushBoth {
                nodes.Push(node.rightNode)
            }
            nodes.Push(node.leftNode)
        } else {
            if pushBoth {
                nodes.Push(node.leftNode)
            }
            nodes.Push(node.rightNode)
        }
    }

    done <- struct{}{}
}

func newBtree(index *btreeIndex) *btreeNode {
    // Worst O(n) to sample initial points
    pointIds := math.RandomDistinctInts(2, len(index.items))
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
    pointIds := math.RandomDistinctInts(2, len(ids))
    pointA := index.items[ids[pointIds[0]]]
    pointB := index.items[ids[pointIds[1]]]
    split := math.EquidistantPlane(pointA, pointB)

    var leftIds, rightIds []int
    for _, id := range ids {
        if math.PointPlaneDistance(index.Get(id), split) <= 0 {
            leftIds = append(leftIds, id)
        } else {
            rightIds = append(rightIds, id)
        }
    }

    return &btreeNode{value: split}, leftIds, rightIds
}

func (bt *btreeNode) isLeaf() bool {
    return (bt.leftNode == nil) && (bt.rightNode == nil)
}
