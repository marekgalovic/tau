package tau

import (
    "net";
    "math";
    "context";
    "errors";
    "path/filepath";
    "sync";
    "sort";

    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";
    
    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
    "google.golang.org/grpc";
    log "github.com/Sirupsen/logrus";
)

const (
    EventNodeCreated int32 = 1
    EventNodeDeleted int32 = 2
)

type Cluster interface {
    Uuid() string
    NodesCount() (int, error)
    ListNodes() ([]Node, error)
    GetNode(string) (Node, error)
    GetHrwNode(string) (Node, error)
    GetTopHrwNodes(int, string) (utils.Set, error)
    NodeChanges() <-chan interface{}
}

type Node interface {
    Meta() *pb.Node
    Dial() (*grpc.ClientConn, error)
}

type cluster struct {
    ctx context.Context
    config *Config
    zk *zk.Conn

    uuid string
    seqId int64

    nodes map[string]Node
    nodesMutex *sync.Mutex
    nodeChangesNotifications utils.Broadcast

    connCache map[string]*grpc.ClientConn
}

type node struct {
    meta *pb.Node
    cluster *cluster
}

type NodesChangedNotification struct {
    Event int32
    Node Node
}

type hrwNodeScore struct {
    uuid string
    score float64
}

func newNode(meta *pb.Node, cluster *cluster) Node {
    return &node {
        meta: meta,
        cluster: cluster,
    }
}

func NewCluster(ctx context.Context, config *Config, zkConn *zk.Conn) (Cluster, error) {
    uuid, err := utils.VolatileNodeUuid()
    if err != nil {
        return nil, err
    }

    c := &cluster{
        ctx: ctx,
        config: config,
        zk: zkConn,
        uuid: uuid,
        nodes: make(map[string]Node),
        nodesMutex: &sync.Mutex{},
        nodeChangesNotifications: utils.NewThreadSafeBroadcast(),
        connCache: make(map[string]*grpc.ClientConn),
    }

    if err := c.bootstrapZk(); err != nil {
        return nil, err
    }
    if err := c.register(); err != nil {
        return nil, err
    }

    go c.watchNodes()
    go c.closeClientConnections()

    return c, nil
}

func (c *cluster) bootstrapZk() error {
    paths := []string{
        c.zkNodesPath(),
    }

    for _, path := range paths {
        err := utils.ZkCreatePath(c.zk, path, nil, int32(0), zk.WorldACL(zk.PermAll))
        if err == zk.ErrNodeExists {
            continue
        }
        if err != nil {
            return err
        } 
    }
    return nil
}

func (c *cluster) register() error {
    ipAddress, err := utils.NodeIpAddress()
    if err != nil {
        return err
    }

    nodeData, err := proto.Marshal(&pb.Node{Uuid: c.uuid, IpAddress: ipAddress, Port: c.config.Server.Port})
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

func (c *cluster) watchNodes() {
    for {
        nodes, _, event, err := c.zk.ChildrenW(c.zkNodesPath())
        if err != nil {
            panic(err)
        }
        if err := c.updateNodes(nodes); err != nil {
            panic(err)
        }

        select {
        case <- event:
        case <- c.ctx.Done():
            return
        }
    }
}

func (c *cluster) updateNodes(nodes []string) error {
    updatedNodes := utils.NewSet()
    for _, nodeName := range nodes {
        updatedNodes.Add(nodeName)

        if _, exists := c.nodes[nodeName]; !exists {
            node, err := c.GetNode(nodeName)
            if err != nil {
                return err
            }
            c.nodesMutex.Lock()
            c.nodes[nodeName] = node
            c.nodesMutex.Unlock()

            c.nodeChangesNotifications.Send(&NodesChangedNotification{Event: EventNodeCreated, Node: node})
            log.WithFields(log.Fields{
                "uuid": node.Meta().GetUuid(),
                "ip": node.Meta().GetIpAddress(),
            }).Info("New cluster node")
        }
    }

    defer c.nodesMutex.Unlock()
    c.nodesMutex.Lock()
    for nodeName, node := range c.nodes {
        if !updatedNodes.Contains(nodeName) {
            delete(c.nodes, nodeName)
            c.nodeChangesNotifications.Send(&NodesChangedNotification{Event: EventNodeDeleted, Node: node})
            log.WithFields(log.Fields{
                "uuid": node.Meta().GetUuid(),
            }).Info("Cluster node deleted")
        }
    }
    return nil
}

func (c *cluster) closeClientConnections() {
    select {
    case <- c.ctx.Done():
        for _, conn := range c.connCache {
            if err := conn.Close(); err != nil {
                log.Error(err)
            }
        }
    }
}

func (c *cluster) Uuid() string {
    return c.uuid
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
            return nil, err
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

func (c *cluster) NodeChanges() <-chan interface{} {
    return c.nodeChangesNotifications.Listen(10)
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
