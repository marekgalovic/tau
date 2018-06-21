package tau

import (
    "fmt";
    "math/rand";

    "github.com/marekgalovic/tau/math"
)

type Index interface {
    Build()  // Implementation specific
    Search([]float32) SearchResult  // Implementation specific
    Len() int
    Items() map[int][]float32
    Add(int, []float32) error
    Get(int) []float32
    ComputeDistance([]float32, []float32) float32
    // Load(string) error
    // Save(string) error
}

type baseIndex struct {
    size int
    metric string
    items map[int][]float32
}

func newBaseIndex(size int, metric string) baseIndex {
    return baseIndex{
        size: size,
        metric: metric,
        items: make(map[int][]float32),
    }
}

func (i *baseIndex) Len() int {
    return len(i.items)
}

func (i *baseIndex) Items() map[int][]float32 {
    return i.items
}

func (i *baseIndex) Add(id int, vec []float32) error {
    if len(vec) != i.size {
        return fmt.Errorf("Vector with %d components does not match index size %d", len(vec), i.size)
    }
    if _, exists := i.items[id]; exists {
        return fmt.Errorf("Id: %d already exists", id)
    }

    i.items[id] = vec
    return nil
}

func (i *baseIndex) Get(id int) []float32 {
    return i.items[id]
}

func (i *baseIndex) ComputeDistance(a, b []float32) float32 {
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

func (i *baseIndex) randomItem() []float32 {
    return i.items[rand.Intn(len(i.items))]
}

