package tau

import (
    "net";
    "path/filepath";

    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";
    
    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
    "google.golang.org/grpc";
)

type Cluster interface {
    Close() error
    Register() error
    NodesCount() (int, error)
    ListNodes() ([]Node, error)
    GetNode(string) (Node, error)
}

type Node interface {
    Meta() *pb.Node
    Conn() (*grpc.ClientConn, error)
}

type cluster struct {
    config *Config
    zk *zk.Conn
    seqId int64

    connCache map[string]*grpc.ClientConn
}

type node struct {
    meta *pb.Node
    cluster *cluster
}

func newNode(meta *pb.Node, cluster *cluster) Node {
    return &node {
        meta: meta,
        cluster: cluster,
    }
}

func NewCluster(config *Config, zkConn *zk.Conn) (Cluster, error) {
    c := &cluster{
        config: config,
        zk: zkConn,
        connCache: make(map[string]*grpc.ClientConn),
    }

    if err := c.bootstrapZk(); err != nil {
        return nil, err
    }
    return c, nil
}

func (c *cluster) bootstrapZk() error {
    paths := []string{
        c.config.Zookeeper.BasePath,
        c.zkNodesPath(),
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

func (c *cluster) closeClientConnections() error {
    for _, conn := range c.connCache {
        if err := conn.Close(); err != nil {
            return err
        }
    }
    return nil
}

func (c *cluster) Close() error {
    if err := c.closeClientConnections(); err != nil {
        return err
    }
    return nil
}

func (c *cluster) Register() error {
    uuid, err := utils.NodeUuid()
    if err != nil {
        return err
    }
    ipAddress, err := utils.NodeIpAddress()
    if err != nil {
        return err
    }

    nodeData, err := proto.Marshal(&pb.Node{Uuid: uuid, IpAddress: ipAddress})
    if err != nil {
        return err
    }
    nodeName, err := c.zk.CreateProtectedEphemeralSequential(filepath.Join(c.zkNodesPath(), "n"), nodeData, zk.WorldACL(zk.PermAll))
    if err != nil {
        return err
    }

    c.seqId, err = utils.ParseSeqId(nodeName)

    return err
}

func (c *cluster) NodesCount() (int, error) {
    nodeNames, _, err := c.zk.Children(c.zkNodesPath())
    if err != nil {
        return 0, err
    }
    return len(nodeNames), nil
}

func (c *cluster) ListNodes() ([]Node, error) {
    nodeNames, _, err := c.zk.Children(c.zkNodesPath())
    if err != nil {
        return nil, err
    }

    nodes := make([]Node, len(nodeNames))
    for i, nodeName := range nodeNames {
        var err error
        if nodes[i], err = c.GetNode(nodeName); err != nil {
            return nil,  err
        }
    }

    return nodes, nil
}

func (c *cluster) GetNode(nodeName string) (Node, error) {
    nodeData, _, err := c.zk.Get(filepath.Join(c.zkNodesPath(), nodeName))
    if err != nil {
        return nil, err
    }

    node := &pb.Node{}
    if err = proto.Unmarshal(nodeData, node); err != nil {
        return nil, err
    }

    return newNode(node, c), nil
}

func (c *cluster) dialNode(address string) (*grpc.ClientConn, error) {
    if conn, exists := c.connCache[address]; exists {
        return conn, nil
    }

    var err error
    c.connCache[address], err = grpc.Dial(net.JoinHostPort(address, c.config.Server.Port), grpc.WithInsecure())
    if err != nil {
        return nil, err
    }

    return c.connCache[address], nil
}

func (c *cluster) zkNodesPath() string {
    return filepath.Join(c.config.Zookeeper.BasePath, "nodes")
}

func (n *node) Meta() *pb.Node {
    return n.meta
}

func (n *node) Conn() (*grpc.ClientConn, error) {
    return n.cluster.dialNode(n.meta.IpAddress)
}
