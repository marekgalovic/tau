package main

import (
    "github.com/marekgalovic/tau";
    "github.com/marekgalovic/tau/cluster";
    "github.com/marekgalovic/tau/dataset";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/utils";

    log "github.com/Sirupsen/logrus";
)

func main() {
    config := tau.NewConfig()
    // config.Server.Port = "5556"

    zookeeper, err := utils.NewZookeeper(config.Zookeeper)
    if err != nil {
        log.Fatal(err)
    }
    defer zookeeper.Close()

    cluster, err := cluster.NewCluster(zookeeper)
    if err != nil {
        log.Fatal(err)
    }
    defer cluster.Close()

    lStorage := storage.NewLocal()
    datasetsManager, err := dataset.NewManager(zookeeper, cluster, lStorage)
    if err != nil {
        log.Fatal(err)
    }
    datasetsManager.Run()
    // defer datasetsManager.Close()

    server := tau.NewServer(config, zookeeper, datasetsManager, lStorage)
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }

    <- utils.InterruptSignal()
    server.Stop()
}
