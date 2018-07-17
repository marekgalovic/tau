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

    cluster, err := cluster.NewCluster(cluster.ClusterConfig{Ip: config.Server.Address, Port: config.Server.Port}, zookeeper)
    if err != nil {
        log.Fatal(err)
    }
    defer cluster.Close()

    lStorage := storage.NewLocal()
    datasetsManager, err := dataset.NewManager(config.Dataset, zookeeper, cluster, lStorage)
    if err != nil {
        log.Fatal(err)
    }
    datasetsManager.Run()

    server := tau.NewServer(config, zookeeper, datasetsManager, lStorage)
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }

    <- utils.InterruptSignal()
    server.Stop()
}
