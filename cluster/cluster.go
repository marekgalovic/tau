package cluster

import (
    "math";
    "path/filepath";

    pb "github.com/marekgalovic/tau/protobuf";
    
    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
)

const (
    ZkBasePath string = "/tau";
)

func zkPath(path string) string {
    return filepath.Join(ZkBasePath, path)
}

type Cluster interface {
    Register() error
    GetMaster() (*pb.Node, error)
    IsMaster() (bool, error)
    NodesCount() (int, error)
    ListNodes() ([]*pb.Node, error)
}

type cluster struct {
    zk *zk.Conn
    seqId int64
}

func NewCluster(zkConn *zk.Conn) (Cluster, error) {
    c := &cluster{
        zk: zkConn,
    }

    if err := c.bootstrapZk(); err != nil {
        return nil, err
    }
    return c, nil
}

func (c *cluster) bootstrapZk() error {
    paths := []string{
        ZkBasePath,
        zkPath("nodes"),
    }

    for _, path := range paths {
        exists, _, err := c.zk.Exists(path)
        if err != nil {
            return err
        }
        if !exists {
            _, err = c.zk.Create(path, nil, int32(0), zk.WorldACL(zk.PermAll))
            if err != nil {
                return err
            }
        }   
    }
    return nil
}

func (c *cluster) Register() error {
    uuid, err := NodeUuid()
    if err != nil {
        return err
    }
    ipAddress, err := NodeIpAddress()
    if err != nil {
        return err
    }

    nodeData, err := proto.Marshal(&pb.Node{Uuid: uuid, IpAddress: ipAddress})
    if err != nil {
        return err
    }
    nodeName, err := c.zk.CreateProtectedEphemeralSequential(zkPath("nodes/n"), nodeData, zk.WorldACL(zk.PermAll))
    if err != nil {
        return err
    }

    c.seqId, err = ParseSeqId(nodeName)

    return err
}

func (c *cluster) GetMaster() (*pb.Node, error) {
    nodeNames, _, err := c.zk.Children(zkPath("nodes"))
    if err != nil {
        return nil, err
    }

    minSeqId := int64(math.MaxInt64)
    var masterNodeName string
    for _, nodeName := range nodeNames {
        seqId, err := ParseSeqId(nodeName)
        if err != nil {
            return nil, err
        }
        if seqId < minSeqId {
            minSeqId = seqId
            masterNodeName = nodeName
        }
    }

    return c.GetNode(masterNodeName)
}

func (c *cluster) IsMaster() (bool, error) {
    nodeNames, _, err := c.zk.Children(zkPath("nodes"))
    if err != nil {
        return false, err
    }

    if len(nodeNames) == 1 {
        return true, nil
    }

    maxSeqId := int64(math.MinInt64)
    var previousCandidateNodeName string
    for _, nodeName := range nodeNames {
        seqId, err := ParseSeqId(nodeName)
        if err != nil {
            return false, err
        }
        if (seqId > maxSeqId) && (seqId < c.seqId) {
            maxSeqId = seqId
            previousCandidateNodeName = nodeName
        }
    }

    for {
        _, _, eventChan, err := c.zk.ExistsW(filepath.Join(zkPath("nodes"), previousCandidateNodeName))
        if err != nil {
            return false, err
        }
        event := <- eventChan

        if event.Type == zk.EventNodeDeleted {
            return true, nil
        }
    }
}

func (c *cluster) NodesCount() (int, error) {
    nodeNames, _, err := c.zk.Children(zkPath("nodes"))
    if err != nil {
        return 0, err
    }
    return len(nodeNames), nil
}

func (c *cluster) ListNodes() ([]*pb.Node, error) {
    nodeNames, _, err := c.zk.Children(zkPath("nodes"))
    if err != nil {
        return nil, err
    }

    nodes := make([]*pb.Node, len(nodeNames))
    for i, nodeName := range nodeNames {
        var err error
        if nodes[i], err = c.GetNode(nodeName); err != nil {
            return nil,  err
        }
    }

    return nodes, nil
}

func (c *cluster) GetNode(nodeName string) (*pb.Node, error) {
    nodeData, _, err := c.zk.Get(filepath.Join(zkPath("nodes"), nodeName))
    if err != nil {
        return nil, err
    }

    node := &pb.Node{}
    if err = proto.Unmarshal(nodeData, node); err != nil {
        return nil, err
    }

    return node, nil
}
