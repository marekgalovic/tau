package main

import (
    "github.com/marekgalovic/tau/pkg";
    "github.com/marekgalovic/tau/pkg/cluster";
    "github.com/marekgalovic/tau/pkg/dataset";
    "github.com/marekgalovic/tau/pkg/storage";
    "github.com/marekgalovic/tau/pkg/utils";

    log "github.com/Sirupsen/logrus";
)

func main() {
    config, err := tau.NewConfig()
    if err != nil {
        log.Fatal(err)
    }

    uuid, err := utils.VolatileNodeUuid()
    if err != nil {
        log.Fatal(err)
    }

    zookeeper, err := utils.NewZookeeper(config.Zookeeper)
    if err != nil {
        log.Fatal(err)
    }
    defer zookeeper.Close()

    cluster, err := cluster.NewCluster(cluster.ClusterConfig{Uuid: uuid, Ip: config.Server.Address, Port: config.Server.Port}, zookeeper)
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
