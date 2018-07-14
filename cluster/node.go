package cluster

import (
    "net";

    pb "github.com/marekgalovic/tau/protobuf";
    
    "google.golang.org/grpc";
)

type Node interface {
    Meta() *pb.Node
    Dial() (*grpc.ClientConn, error)
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

func (n *node) Meta() *pb.Node {
    return n.meta
}

func (n *node) Dial() (*grpc.ClientConn, error) {
    return n.cluster.dialNode(net.JoinHostPort(n.Meta().IpAddress, n.Meta().Port))
}
