package main

import (
    "time";
    "context";
    
    "github.com/marekgalovic/tau";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/utils";

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

    lStorage := storage.NewLocal()
    datasetsManager, err := tau.NewDatasetsManager(ctx, config, zkConn, cluster, lStorage)
    if err != nil {
        log.Fatal(err)
    }
    if err := datasetsManager.Run(); err != nil {
        log.Fatal(err)
    }

    server := tau.NewServer(config, zkConn, datasetsManager, lStorage)
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }

    <- utils.InterruptSignal()
    server.Stop()
    closeFunc()
}
