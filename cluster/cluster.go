package cluster

import (
    // "fmt";
    "time";
    "path/filepath";

    pb "github.com/marekgalovic/tau/protobuf";
    
    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
)

const (
    zkNamespace string = "/tau";
)

type Cluster interface {
    Register() error
    NodesCount() (int, error)
    GetNodes() ([]*pb.Node, error)
}

type cluster struct {
    zk *zk.Conn
}

func NewCluster(zkServers []string, timeout time.Duration) (Cluster, error) {
    conn, _, err := zk.Connect(zkServers, timeout)
    if err != nil {
        return nil, err
    }

    nm := &cluster{
        zk: conn,
    }
    if err = nm.bootstrapZk(); err != nil {
        return nil, err
    }
    return nm, nil
}

func (nm *cluster) bootstrapZk() error {
    paths := []string{
        zkNamespace,
        nm.zkPath("nodes"),
    }

    for _, path := range paths {
        exists, _, err := nm.zk.Exists(path)
        if err != nil {
            return err
        }
        if !exists {
            _, err = nm.zk.Create(path, nil, int32(0), zk.WorldACL(zk.PermAll))
            if err != nil {
                return err
            }
        }   
    }
    return nil
}

func (nm *cluster) Register() error {
    nodeData, err := proto.Marshal(&pb.Node{Address: "127.0.0.1"})
    if err != nil {
        return err
    }
    _, err = nm.zk.CreateProtectedEphemeralSequential(nm.zkPath("nodes/node"), nodeData, zk.WorldACL(zk.PermAll));
    if err != nil {
        return err
    }
    return nil
}

func (nm *cluster) NodesCount() (int, error) {
    nodeNames, _, err := nm.zk.Children(nm.zkPath("nodes"))
    if err != nil {
        return 0, err
    }
    return len(nodeNames), nil
}

func (nm *cluster) GetNodes() ([]*pb.Node, error) {
    nodeNames, _, err := nm.zk.Children(nm.zkPath("nodes"))
    if err != nil {
        return nil, err
    }

    nodes := make([]*pb.Node, len(nodeNames))
    for i, nodeName := range nodeNames {
        nodeData, _, err := nm.zk.Get(filepath.Join(nm.zkPath("nodes"), nodeName))
        if err != nil {
            return nil, err
        }

        nodes[i] = &pb.Node{}
        if err = proto.Unmarshal(nodeData, nodes[i]); err != nil {
            return nil, err
        }
    }

    return nodes, nil
}

func (nm *cluster) zkPath(path string) string {
    return filepath.Join(zkNamespace, path)
}
