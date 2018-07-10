// Rendezvous (highest random weight) hashing
package utils

import (
    "math";
    "hash/crc32";
    "sort";
    "sync";
    "errors";
)

var crc32Table *crc32.Table = crc32.MakeTable(crc32.Castagnoli)

var (
    ErrRendezvousNoNodes error = errors.New("No nodes")
    ErrRendezvousNotEnoughNodes error = errors.New("Number of available nodes is less than the number of requested nodes")
)

type RendezvousHash interface {
    Add(string, float32)
    Delete(string)
    Get(string) (*RendezvousNodeScore, error)
    GetN(int, string) ([]*RendezvousNodeScore, error)
}

type RendezvousNodeScore struct {
    Node string
    Score float32
}

type rendezvousHash struct {
    nodes map[string]float32
    nodesMutex *sync.Mutex
}

func NewRendezvousHash() RendezvousHash {
    return &rendezvousHash{
        nodes: make(map[string]float32),
        nodesMutex: &sync.Mutex{},
    }
}

func RendezvousHashScore(node, key string, weight float32) float32 {
    sum := crc32.Checksum(append([]byte(node), []byte(key)...), crc32Table)

    return weight * float32(-math.Log(float64(sum / 0xFFFFFFFF)))
}

func (rh *rendezvousHash) Add(node string, weight float32) {
    defer rh.nodesMutex.Unlock()
    rh.nodesMutex.Lock()

    rh.nodes[node] = weight
}

func (rh *rendezvousHash) Delete(node string) {
    defer rh.nodesMutex.Unlock()
    rh.nodesMutex.Lock()
    
    delete(rh.nodes, node)
}

func (rh *rendezvousHash) Get(key string) (*RendezvousNodeScore, error) {
    if len(rh.nodes) == 0 {
        return nil, ErrRendezvousNoNodes
    }

    var maxScore float32 = -math.MaxFloat32
    var topNode string
    for node, nodeWeight := range rh.nodes {
        if score := RendezvousHashScore(node, key, nodeWeight); score > maxScore {
            maxScore = score
            topNode = node
        }
    }

    return &RendezvousNodeScore{Node: topNode, Score: maxScore}, nil
}

func (rh *rendezvousHash) GetN(n int, key string) ([]*RendezvousNodeScore, error) {
    if len(rh.nodes) == 0 {
        return nil, ErrRendezvousNoNodes
    }
    if n > len(rh.nodes) {
        return nil, ErrRendezvousNotEnoughNodes
    }
    if n < 0 {
        n = len(rh.nodes)
    }

    scores := make([]*RendezvousNodeScore, len(rh.nodes))
    i := 0
    for node, nodeWeight := range rh.nodes {
        scores[i] = &RendezvousNodeScore {
            Node: node,
            Score: RendezvousHashScore(node, key, nodeWeight),
        }
        i++
    }

    sort.SliceStable(scores, func(i, j int) bool {
        return scores[i].Score > scores[j].Score
    })
    return scores[:n], nil
}
