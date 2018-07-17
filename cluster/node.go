package cluster

import (
    "net";

    pb "github.com/marekgalovic/tau/protobuf";
    
    "google.golang.org/grpc";
)

type Node interface {
    Meta() *pb.Node
    Address() string
    Dial() (*grpc.ClientConn, error)
}

type node struct {
    meta *pb.Node
    cluster *cluster
}

func NewNode(meta *pb.Node, cluster *cluster) Node {
    return &node {
        meta: meta,
        cluster: cluster,
    }
}

func (n *node) Meta() *pb.Node {
    return n.meta
}

func (n *node) Address() string {
    return net.JoinHostPort(n.Meta().GetIpAddress(), n.Meta().GetPort())
}

func (n *node) Dial() (*grpc.ClientConn, error) {
    return n.cluster.dialNode(n.Meta().GetUuid())
}
