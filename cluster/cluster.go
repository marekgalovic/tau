package cluster

import (
    "context";
    "sync";
    "path/filepath";

    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
    "google.golang.org/grpc";
    log "github.com/Sirupsen/logrus";
)

const zkNodesPath string = "nodes"

const (
    EventNodeCreated int32 = 1
    EventNodeDeleted int32 = 2
)

type NodesChangedNotification struct {
    Event int32
    Node Node
}

type Cluster interface {
    Close()
    Uuid() string
    ListNodes() ([]Node, error)
    GetNode(string) (Node, error)
    GetHrwNode(string) (Node, error)
    GetTopHrwNodes(int, string) (utils.Set, error)
    NodeChanges() <-chan interface{}
}

type cluster struct {
    uuid string
    ctx context.Context
    cancel context.CancelFunc
    zk utils.Zookeeper

    nodes map[string]Node
    nodesMutex *sync.Mutex
    nodeChangesNotifications utils.Broadcast

    connCache map[string]*grpc.ClientConn
    connCacheMutex *sync.Mutex
}

func NewCluster(zk utils.Zookeeper) (Cluster, error) {
    uuid, err := utils.VolatileNodeUuid()
    if err != nil {
        return nil, err
    }

    ctx, cancel := context.WithCancel(context.Background())
    c := &cluster {
        uuid: uuid,
        ctx: ctx,
        cancel: cancel,
        zk: zk,
        nodes: make(map[string]Node),
        nodesMutex: &sync.Mutex{},
        nodeChangesNotifications: utils.NewThreadSafeBroadcast(),
        connCache: make(map[string]*grpc.ClientConn),
        connCacheMutex: &sync.Mutex{},
    }

    if err := c.bootstrapZk(); err != nil {
        return nil, err
    }
    if err := c.register(); err != nil {
        return nil, err
    }

    go c.watchNodes()

    return c, nil
}

func (c *cluster) bootstrapZk() error {
    paths := []string{zkNodesPath}

    for _, path := range paths {
        _, err := c.zk.CreatePath(path, nil, int32(0))
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

    nodeData, err := proto.Marshal(&pb.Node{Uuid: c.Uuid(), IpAddress: ipAddress, Port: "5555"})
    if err != nil {
        return err
    }
    _, err = c.zk.CreateProtectedEphemeralSequential(filepath.Join(zkNodesPath, "n"), nodeData)

    return err
}

func (c *cluster) getNode(zNode string) (Node, bool) {
    defer c.nodesMutex.Unlock()
    c.nodesMutex.Lock()

    node, exists := c.nodes[zNode]
    return node, exists
}

func (c *cluster) addNode(zNode string, node Node) {
    defer c.nodesMutex.Unlock()
    c.nodesMutex.Lock()

    c.nodes[zNode] = node   
}

func (c *cluster) deleteNode(zNode string) {
    defer c.nodesMutex.Unlock()
    c.nodesMutex.Lock()

    delete(c.nodes, zNode)
}

func (c *cluster) watchNodes() {
    changes, errors := c.zk.ChildrenChanges(c.ctx, zkNodesPath)

    for {
        select {
        case event := <-changes:
            switch event.Type {
            case utils.EventZkWatchInit, utils.EventZkNodeCreated:
                node, err := c.GetNode(event.ZNode)
                if err != nil {
                    panic(err)
                }

                c.addNode(event.ZNode, node)
                c.nodeChangesNotifications.Send(&NodesChangedNotification {
                    Event: EventNodeCreated,
                    Node: node,
                })
            case utils.EventZkNodeDeleted:
                node, _ := c.getNode(event.ZNode)

                c.deleteNode(event.ZNode)
                c.nodeChangesNotifications.Send(&NodesChangedNotification {
                    Event: EventNodeDeleted,
                    Node: node,
                })
            }
        case err := <-errors:
            panic(err)
        case <-c.ctx.Done():
            return
        }
    }
}

func (c *cluster) Close() {
    c.cancel()

    defer c.connCacheMutex.Unlock()
    c.connCacheMutex.Lock()
    for _, conn := range c.connCache {
        if err := conn.Close(); err != nil {
            log.Error(err)
        }
    }
}

func (c *cluster) Uuid() string {
    return c.uuid
}

func (c *cluster) ListNodes() ([]Node, error) {
    nodeNames, err := c.zk.Children(zkNodesPath)
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
    path := filepath.Join(zkNodesPath, nodeName)
    nodeData, err := c.zk.Get(path)
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

func (c *cluster) NodeChanges() <-chan interface{} {
    return c.nodeChangesNotifications.Listen(10)
}

func (c *cluster) dialNode(address string) (*grpc.ClientConn, error) {
    c.connCacheMutex.Lock()
    if conn, exists := c.connCache[address]; exists {
        c.connCacheMutex.Unlock()
        return conn, nil
    }

    conn, err := grpc.Dial(address, grpc.WithInsecure())
    if err != nil {
        return nil, err
    }

    c.connCacheMutex.Lock()
    c.connCache[address] = conn
    c.connCacheMutex.Unlock()

    return conn, nil
}
