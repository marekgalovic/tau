package tau

import (
    "net";
    "math";
    "path/filepath";

    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";
    
    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
    "google.golang.org/grpc";
    log "github.com/Sirupsen/logrus";
)

type Cluster interface {
    Close() error
    NotifyWhenMaster() <-chan bool
    NodesCount() (int, error)
    ListNodes() ([]Node, error)
    GetNode(string) (Node, error)
}

type Node interface {
    Meta() *pb.Node
    Dial() (*grpc.ClientConn, error)
}

type cluster struct {
    config *Config
    zk *zk.Conn
    seqId int64
    stop chan struct{}

    isMasterNotif chan bool
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
        stop: make(chan struct{}),
        isMasterNotif: make(chan bool),
        connCache: make(map[string]*grpc.ClientConn),
    }

    if err := c.bootstrapZk(); err != nil {
        return nil, err
    }
    if err := c.register(); err != nil {
        return nil, err
    }

    go c.watchMaster()

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

func (c *cluster) register() error {
    uuid, err := utils.NodeUuid()
    if err != nil {
        return err
    }
    ipAddress, err := utils.NodeIpAddress()
    if err != nil {
        return err
    }

    nodeData, err := proto.Marshal(&pb.Node{Uuid: uuid, IpAddress: ipAddress, Port: c.config.Server.Port})
    if err != nil {
        return err
    }
    nodePath, err := c.zk.CreateProtectedEphemeralSequential(filepath.Join(c.zkNodesPath(), "n"), nodeData, zk.WorldACL(zk.PermAll))
    if err != nil {
        return err
    }

    c.seqId, err = utils.ParseSeqId(nodePath)
    return err
}

func (c *cluster) watchMaster() {
    for {
        nodes, err := c.ListNodes()
        if err != nil {
            panic(err)
        }

        if len(nodes) == 0 {
            panic("No nodes")
        }

        var leaderSeqId, candidateSeqId int64 = math.MaxInt64, math.MaxInt64
        var leader, candidate Node
        for _, node := range nodes {
            seqId := node.Meta().GetSeqId()
            if seqId < leaderSeqId {
                leaderSeqId = seqId
                leader = node
            }
            if (seqId < c.seqId) && (seqId < candidateSeqId) {
                candidateSeqId = seqId
                candidate = node
            }
        }

        if c.seqId == leader.Meta().GetSeqId() {
            log.Info("Node is master")
            c.isMasterNotif <- true
            return
        }

        if candidate == nil {
            panic("No candidate node")
        }

        exists, _, event, err := c.zk.ExistsW(candidate.Meta().GetZkPath())
        if err != nil {
            panic("Failed to get candidate")
        }
        if !exists {
            panic("Candidate node does not exist")
        }

        log.Infof("Following candidate master: `%s`", candidate.Meta().GetZkPath())
        select {
        case <- event:
            continue
        case <- c.stop:
            return
        }
    }
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
    close(c.stop)

    if err := c.closeClientConnections(); err != nil {
        return err
    }
    return nil
}

func (c *cluster) NotifyWhenMaster() <-chan bool {
    return c.isMasterNotif
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
    path := filepath.Join(c.zkNodesPath(), nodeName)
    nodeData, _, err := c.zk.Get(path)
    if err != nil {
        return nil, err
    }

    node := &pb.Node{}
    if err = proto.Unmarshal(nodeData, node); err != nil {
        return nil, err
    }
    node.ZkPath = path

    if node.SeqId, err = utils.ParseSeqId(nodeName); err != nil {
        return nil, err
    }

    return newNode(node, c), nil
}

func (c *cluster) dialNode(address string) (*grpc.ClientConn, error) {
    if conn, exists := c.connCache[address]; exists {
        return conn, nil
    }

    var err error
    c.connCache[address], err = grpc.Dial(address, grpc.WithInsecure())
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

func (n *node) Dial() (*grpc.ClientConn, error) {
    return n.cluster.dialNode(net.JoinHostPort(n.Meta().IpAddress, n.Meta().Port))
}
