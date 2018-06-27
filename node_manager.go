package tau

type Node struct{}

type NodeManager interface {
    GetNodes() ([]*Node, error)
}

type nodeManager struct {}

func NewNodeManager() NodeManager {
    return &nodeManager{}
}

func (nm *nodeManager) GetNodes() ([]*Node, error) {
    return []*Node{}, nil
}
