package cluster

import (
    "math";
    "sort";
    "errors";

    "github.com/marekgalovic/tau/pkg/utils";
)

type hrwNodeScore struct {
    uuid string
    score float64
}

func (c *cluster) GetHrwNode(key string) (Node, error) {
    defer c.nodesMutex.Unlock()
    c.nodesMutex.Lock()

    if len(c.nodes) == 0 {
        return nil, errors.New("No nodes")
    }

    var maxScore float64 = -math.MaxFloat64
    var topNode Node
    for _, node := range c.nodes {
        if score := utils.RendezvousHashScore(node.Meta().GetUuid(), key, 1); score > maxScore {
            maxScore = score
            topNode = node
        }
    }
    return topNode, nil
}

func (c *cluster) GetTopHrwNodes(n int, key string) (utils.Set, error) {
    defer c.nodesMutex.Unlock()
    c.nodesMutex.Lock()
    
    if len(c.nodes) == 0 {
        return nil, errors.New("No nodes")
    }

    scores := make([]*hrwNodeScore, len(c.nodes))
    i := 0
    for _, node := range c.nodes {
        scores[i] = &hrwNodeScore {
            uuid: node.Meta().GetUuid(),
            score: utils.RendezvousHashScore(node.Meta().GetUuid(), key, 1),
        }
        i++
    }
    sort.Slice(scores, func(i, j int) bool {
        return scores[i].score > scores[j].score
    })

    result := utils.NewSet()
    for i := 0; i < n; i++ {
        if i < len(c.nodes) {
            result.Add(scores[i].uuid)
            continue
        }
        break
    }
    return result, nil
}
