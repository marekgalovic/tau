package cluster

import (
    "context";
    "sync";
    "path/filepath";
    "time";

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

type ClusterConfig struct {
    Ip string
    Port string
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
    config ClusterConfig
    zk utils.Zookeeper

    nodes map[string]Node
    nodesMutex *sync.Mutex
    nodeChangesNotifications utils.Broadcast

    connCache map[string]*grpc.ClientConn
    connCacheMutex *sync.Mutex

    log *log.Entry
}

func NewCluster(config ClusterConfig, zk utils.Zookeeper) (Cluster, error) {
    uuid, err := utils.VolatileNodeUuid()
    if err != nil {
        return nil, err
    }

    ctx, cancel := context.WithCancel(context.Background())
    c := &cluster {
        uuid: uuid,
        ctx: ctx,
        cancel: cancel,
        config: config,
        zk: zk,
        nodes: make(map[string]Node),
        nodesMutex: &sync.Mutex{},
        nodeChangesNotifications: utils.NewThreadSafeBroadcast(),
        connCache: make(map[string]*grpc.ClientConn),
        connCacheMutex: &sync.Mutex{},
        log: log.WithFields(log.Fields{
            "local_uuid": uuid,
        }),
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
    nodeData, err := proto.Marshal(&pb.Node{Uuid: c.Uuid(), IpAddress: c.config.Ip, Port: c.config.Port})
    if err != nil {
        return err
    }

    _, err = c.zk.Create(filepath.Join(zkNodesPath, c.Uuid()), nodeData, zk.FlagEphemeral)

    return err
}

func (c *cluster) watchNodes() {
    changes, errors := c.zk.ChildrenChanges(c.ctx, zkNodesPath)

    for {
        select {
        case <-c.ctx.Done():
            return
        case event := <-changes:
            switch event.Type {
            case utils.EventZkWatchInit, utils.EventZkNodeCreated:
                node, err := c.GetNode(event.ZNode)
                if err != nil {
                    panic(err)
                }
                c.log.WithFields(log.Fields{
                    "uuid": node.Meta().GetUuid(),
                }).Info("New cluster node")

                c.nodeChangesNotifications.Send(&NodesChangedNotification {
                    Event: EventNodeCreated,
                    Node: node,
                })
            case utils.EventZkNodeDeleted:
                node, err := c.GetNode(event.ZNode)
                if err != nil {
                    panic(err)
                }
                if node == nil {
                    panic("Node does not exists")
                }

                c.log.WithFields(log.Fields{
                    "uuid": node.Meta().GetUuid(),
                }).Info("Cluster node deleted")

                c.nodesMutex.Lock()
                delete(c.nodes, event.ZNode)
                c.nodesMutex.Unlock()

                c.nodeChangesNotifications.Send(&NodesChangedNotification {
                    Event: EventNodeDeleted,
                    Node: node,
                })
            }
        case err := <-errors:
            if (err == zk.ErrClosing) || (err == zk.ErrConnectionClosed) {
                return
            }
            if err != nil {
                panic(err)
            }
        }
    }
}

func (c *cluster) Close() {
    c.cancel()

    defer c.connCacheMutex.Unlock()
    c.connCacheMutex.Lock()
    for _, conn := range c.connCache {
        if err := conn.Close(); err != nil {
            c.log.Error(err)
        }
    }
}

func (c *cluster) Uuid() string {
    return c.uuid
}

func (c *cluster) ListNodes() ([]Node, error) {
    uuids, err := c.zk.Children(zkNodesPath)
    if err != nil {
        return nil, err
    }

    nodes := make([]Node, len(uuids))
    for i, uuid := range uuids {
        var err error
        if nodes[i], err = c.GetNode(uuid); err != nil {
            return nil, err
        }
    }

    return nodes, nil
}

func (c *cluster) GetNode(uuid string) (Node, error) {
    c.nodesMutex.Lock()
    node, exists := c.nodes[uuid]
    c.nodesMutex.Unlock()

    if exists {
        return node, nil
    }

    path := filepath.Join(zkNodesPath, uuid)
    nodeData, err := c.zk.Get(path)
    if err != nil {
        return nil, err
    }

    nodeProto := &pb.Node{}
    if err = proto.Unmarshal(nodeData, nodeProto); err != nil {
        return nil, err
    }

    node = NewNode(nodeProto, c)

    c.nodesMutex.Lock()
    c.nodes[uuid] = node
    c.nodesMutex.Unlock()

    return node, nil
}

func (c *cluster) NodeChanges() <-chan interface{} {
    return c.nodeChangesNotifications.Listen(10)
}

func (c *cluster) dialNode(uuid string) (*grpc.ClientConn, error) {
    c.connCacheMutex.Lock()
    conn, exists := c.connCache[uuid]
    c.connCacheMutex.Unlock()

    if exists {
        return conn, nil
    }

    node, err := c.GetNode(uuid)
    if err != nil {
        return nil, err
    }

    conn, err = grpc.DialContext(c.ctx, node.Address(), grpc.WithInsecure(), grpc.WithTimeout(2 * time.Second), grpc.WithBlock())
    if err != nil {
        return nil, err
    }

    c.connCacheMutex.Lock()
    c.connCache[uuid] = conn
    c.connCacheMutex.Unlock()

    return conn, nil
}
