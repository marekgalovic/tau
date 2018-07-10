package main

import (
    "time";
    "context";
    
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
    // config.Server.Port = "5556"

    zkConn, _, err := zk.Connect(config.Zookeeper.Nodes, 1 * time.Second)
    if err != nil {
        log.Fatal(err)
    }
    defer zkConn.Close()

    ctx, closeFunc := context.WithCancel(context.Background())

    cluster, err := tau.NewCluster(ctx, config, zkConn)
    if err != nil {
        log.Fatal(err)
    }

    datasetsManager, err := tau.NewDatasetsManager(ctx, config, zkConn, cluster, storage.NewLocal())
    if err != nil {
        log.Fatal(err)
    }

    server := tau.NewServer(config, datasetsManager)
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }

    d := &pb.Dataset {
        Name: "b10",
        Path: "./examples/data/random_*",
        NumPartitions: 10,
        NumReplicas: 1,
        Index: index.NewBtreeIndex(256, "Euclidean", 5, 512).ToProto(),
    }

    // log.Info(d)

    if err := datasetsManager.CreateDataset(d); err != nil {
        log.Fatal(err)
    }

    <- time.After(1 * time.Second)

    log.Info(datasetsManager.DeleteDataset("b10"))

    <- utils.InterruptSignal()
    server.Stop()
    closeFunc()
}
