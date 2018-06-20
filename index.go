package tau

import (
    "fmt";
    "math/rand"
)

type Index interface {
    Build()  // Implementation specific
    Search([]float32) (map[int]float32, error)  // Implementation specific
    Len() int
    Add(int, []float32) error
    // Load(string) error
    // Save(string) error
}

type baseIndex struct {
    size int
    items map[int][]float32
}

func newBaseIndex(size int) baseIndex {
    return baseIndex{
        size: size,
        items: make(map[int][]float32),
    }
}

func (i *baseIndex) Len() int {
    return len(i.items)
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

func (i *baseIndex) randomItem() []float32 {
    return i.items[rand.Intn(len(i.items))]
}
