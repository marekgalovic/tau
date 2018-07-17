// Binary search tree
package index

import (
    "fmt";
    "io";
    "time";
    "sync";
    "context";
    "encoding/binary";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/utils";
    pb "github.com/marekgalovic/tau/protobuf";
)

type btreeIndex struct {
    baseIndex
    numTrees int
    maxLeafItems int
    trees []*btreeNode
}

type btreeNode struct {
    value math.Vector
    itemIds []int64
    leftNode *btreeNode
    rightNode *btreeNode
}

type btreeSplitArgs struct {
    parent *btreeNode
    leftIds []int64
    rightIds []int64
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

func (index *btreeIndex) ToProto() *pb.Index {
    proto := index.baseIndex.ToProto()
    proto.Options = &pb.Index_Btree {
        Btree: &pb.BtreeIndexOptions {
            NumTrees: int32(index.numTrees),
            MaxLeafItems: int32(index.maxLeafItems),
        },
    }
    return proto
}

func (index *btreeIndex) Reset() {
    index.trees = make([]*btreeNode, index.numTrees)
    index.baseIndex.Reset()
}

// Build builds the forest.
// As individual trees are independent of each other they
// are built in parallel.
func (index *btreeIndex) Build(ctx context.Context) {
    index.trees = make([]*btreeNode, index.numTrees)
    treeChans := make([]chan *btreeNode, index.numTrees)

    for t := 0; t < index.numTrees; t++ {
        treeChans[t] = make(chan *btreeNode)
        go func(treeChan chan *btreeNode) {
            treeChan <- newBtree(ctx, index)
        }(treeChans[t])
    }

    for i, treeChan := range treeChans {
        index.trees[i] = <- treeChan
        close(treeChan)
    }
}

// Write binary representation of the index
func (index *btreeIndex) Save(writer io.Writer) error {
    if err := index.writeHeader(writer, []byte("tauBTR")); err != nil {
        return err
    }

    // Index specific headers
    if err := binary.Write(writer, binary.LittleEndian, int32(index.numTrees)); err != nil {
        return err
    }
    if err := binary.Write(writer, binary.LittleEndian, int32(index.maxLeafItems)); err != nil {
        return err
    }

    // Index structure
    for _, tree := range index.trees {
        if err := index.writeTree(writer, tree); err != nil {
            return err
        }
    }
    return nil
}

func (index *btreeIndex) writeTree(writer io.Writer, tree *btreeNode) error {
    stack := utils.NewStack()
    stack.Push(tree)

    for stack.Len() > 0 {
        node := stack.Pop().(*btreeNode)

        if err := binary.Write(writer, binary.LittleEndian, node.isLeaf()); err != nil {
            return err
        }
        if node.isLeaf() {
            if err := binary.Write(writer, binary.LittleEndian, int32(len(node.itemIds))); err != nil {
                return err
            }
            if err := binary.Write(writer, binary.LittleEndian, node.itemIds); err != nil {
                return err
            }
        } else {
            if err := binary.Write(writer, binary.LittleEndian, node.value); err != nil {
                return err
            }
            stack.Push(node.rightNode)
            stack.Push(node.leftNode)
        }
    }
    return nil
}

// Read binary representation of the index
func (index *btreeIndex) Load(reader io.Reader) error {
    indexType, err := index.readHeader(reader)
    if err != nil {
        return err
    }
    if string(indexType) != "tauBTR" {
        return fmt.Errorf("Invalid index type `%s`", indexType)
    }

    // Index specific headers
    var numTrees int32
    if err := binary.Read(reader, binary.LittleEndian, &numTrees); err != nil {
        return err
    }
    index.numTrees = int(numTrees)

    var maxLeafItems int32
    if err := binary.Read(reader, binary.LittleEndian, &maxLeafItems); err != nil {
        return err
    }
    index.maxLeafItems = int(maxLeafItems)

    // Index structure
    index.trees = make([]*btreeNode, index.numTrees)
    for i := 0; i < index.numTrees; i++ {
        tree, err := index.readTree(reader)
        if err != nil {
            return err
        }
        index.trees[i] = tree
    }

    return nil
}

func (index *btreeIndex) readTree(reader io.Reader) (*btreeNode, error) {
    tree := &btreeNode{}

    stack := utils.NewStack()
    stack.Push(tree)

    for stack.Len() > 0 {
        node := stack.Pop().(*btreeNode)

        var isLeaf bool
        if err := binary.Read(reader, binary.LittleEndian, &isLeaf); err != nil {
            return nil, err
        }
        if isLeaf {
            var itemsCount int32
            if err := binary.Read(reader, binary.LittleEndian, &itemsCount); err != nil {
                return nil, err
            }
            node.itemIds = make([]int64, itemsCount)
            if err := binary.Read(reader, binary.LittleEndian, &node.itemIds); err != nil {
                return nil, err
            }
        } else {
            node.value = make(math.Vector, index.size + 1) // +1 here because hyperplanes are n+1 dimensional vectors
            if err := binary.Read(reader, binary.LittleEndian, &node.value); err != nil {
                return nil, err
            }
            node.leftNode = &btreeNode{}
            node.rightNode = &btreeNode{}

            stack.Push(node.rightNode)
            stack.Push(node.leftNode)
        }
    }

    return tree, nil
}

// Search traverses trees in the build forest in parallel.
// If query lies on one side of the plane but condition abs(distance) <= log(l2_length(query)) / 10
// is true then other side of the plane is also considered with lower priority.
// Traversal stops once there are numTrees * maxLeafItems candidates. 
func (index *btreeIndex) Search(ctx context.Context, query math.Vector) SearchResult {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    resultsChan := make(chan []int64)
    doneChan := make(chan struct{})
    for _, tree := range index.trees {
        go index.searchTree(tree, query, ctx, resultsChan, doneChan) 
    }

    var nDone int
    resultIds := utils.NewSet()

    SEARCH_LOOP:
    for {
        select {
        case <- ctx.Done():
            break SEARCH_LOOP
        case <- time.After(1 * time.Second):
            break SEARCH_LOOP
        case resultSlice := <- resultsChan:
            for _, id := range resultSlice {
                resultIds.Add(id)
                if resultIds.Len() == index.numTrees * index.maxLeafItems {
                    break SEARCH_LOOP
                }
            }
        case <- doneChan:
            nDone++
            if nDone == len(index.trees) {
                break SEARCH_LOOP
            }
        }
    }

    return newSearchResult(index, query, resultIds)
}

func (index *btreeIndex) searchTree(tree *btreeNode, query math.Vector, ctx context.Context, results chan []int64, done chan struct{}) {
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

func newBtree(ctx context.Context, index *btreeIndex) *btreeNode {
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

        select {
        case <-ctx.Done():
            return nil
        default:
        }
    }
    split := math.EquidistantPlane(pointA, pointB)

    leftIds := make([]int64, 0)
    rightIds := make([]int64, 0)
    for idx, item := range index.items {
        if math.PointPlaneDistance(item, split) <= 0 {
            leftIds = append(leftIds, idx)
        } else {
            rightIds = append(rightIds, idx)
        }

        select {
        case <-ctx.Done():
            return nil
        default:
        }
    }

    root := &btreeNode {value: split}

    stack := utils.NewThreadSafeStack()
    stack.Push(&btreeSplitArgs{root, leftIds, rightIds})
    for stack.Len() > 0 {
        wg := &sync.WaitGroup{}
        args := stack.Pop().(*btreeSplitArgs)

        var leftIds, rightIds []int64
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

        select {
        case <-ctx.Done():
            return nil
        default:
        }
        
        wg.Wait()
    }

    return root
}

func splitSamples(index *btreeIndex, ids []int64) (*btreeNode, []int64, []int64) {
    pointIds := math.RandomDistinctInts(2, len(ids))
    pointA := index.items[ids[pointIds[0]]]
    pointB := index.items[ids[pointIds[1]]]
    split := math.EquidistantPlane(pointA, pointB)

    var leftIds, rightIds []int64
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

func (bt *btreeNode) len() int {
    return len(bt.itemIds)
}
