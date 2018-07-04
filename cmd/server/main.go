package main

import (
    "time";
    
    "github.com/marekgalovic/tau";
    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    log "github.com/Sirupsen/logrus";
)

func main() {
    config := tau.NewConfig()

    zkConn, _, err := zk.Connect(config.Zookeeper.Nodes, 1 * time.Second)
    if err != nil {
        log.Fatal(err)
    }

    cluster, err := tau.NewCluster(config, zkConn)
    if err != nil {
        log.Fatal(err)
    }
    if err := cluster.Register(); err != nil {
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

    if err := datasetsManager.CreateDataset("foo15", "./examples/data/random_*", index.NewBtreeIndex(256, "Euclidean", 5, 512)); err != nil {
        log.Fatal(err)
    }

    <- utils.InterruptSignal()
    server.Stop()
}
