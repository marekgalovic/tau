package tau

import (
    "fmt";
    "math/rand";

    "github.com/marekgalovic/tau/math"
)

type Index interface {
    Build()
    Search(math.Vector) SearchResult
    Len() int
    Items() map[int]math.Vector
    Add(int, math.Vector) error
    Get(int) math.Vector
    ComputeDistance(math.Vector, math.Vector) float64
    // Load(string) error
    // Save(string) error
}

type baseIndex struct {
    size int
    metric string
    items map[int]math.Vector
}

func newBaseIndex(size int, metric string) baseIndex {
    return baseIndex{
        size: size,
        metric: metric,
        items: make(map[int]math.Vector),
    }
}

func (i *baseIndex) Len() int {
    return len(i.items)
}

func (i *baseIndex) Items() map[int]math.Vector {
    return i.items
}

func (i *baseIndex) Add(id int, vec math.Vector) error {
    if len(vec) != i.size {
        return fmt.Errorf("Vector with %d components does not match index size %d", len(vec), i.size)
    }
    if _, exists := i.items[id]; exists {
        return fmt.Errorf("Id: %d already exists", id)
    }

    i.items[id] = vec
    return nil
}

func (i *baseIndex) Get(id int) math.Vector {
    return i.items[id]
}

func (i *baseIndex) ComputeDistance(a, b math.Vector) float64 {
    if i.metric == "Euclidean" {
        return math.EuclideanDistance(a, b)
    }
    if i.metric == "Manhattan" {
        return math.ManhattanDistance(a, b)
    }
    if i.metric == "Cosine" {
        return math.CosineDistance(a, b)
    }
    panic("Invalid metric")
}

func (i *baseIndex) randomItem() math.Vector {
    return i.items[rand.Intn(len(i.items))]
}

