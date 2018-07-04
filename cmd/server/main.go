package main

import (
    "time";
    
    "github.com/marekgalovic/tau";
    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/utils";
    pb "github.com/marekgalovic/tau/protobuf";

    "github.com/samuel/go-zookeeper/zk";
    log "github.com/Sirupsen/logrus";
)

func main() {
    config := tau.NewConfig()
    config.Server.Port = "5556"

    zkConn, _, err := zk.Connect(config.Zookeeper.Nodes, 1 * time.Second)
    if err != nil {
        log.Fatal(err)
    }
    defer zkConn.Close()

    cluster, err := tau.NewCluster(config, zkConn)
    if err != nil {
        log.Fatal(err)
    }
    defer cluster.Close()

    datasetsManager, err := tau.NewDatasetsManager(config, zkConn, cluster, storage.NewLocal())
    if err != nil {
        log.Fatal(err)
    }

    server := tau.NewServer(config, datasetsManager)
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }

    d := &pb.Dataset {
        Name: "foo32",
        Path: "./examples/data/random_*",
        NumPartitions: 10,
        NumReplicas: 1,
        Index: index.NewBtreeIndex(256, "Euclidean", 5, 512).ToProto(),
    }

    if err := datasetsManager.CreateDataset(d); err != nil {
        log.Fatal(err)
    }

    <- utils.InterruptSignal()
    server.Stop()
}
