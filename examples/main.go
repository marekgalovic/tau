package main

import (
    "time";
    
    "github.com/marekgalovic/tau";
    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    // "github.com/marekgalovic/tau/storage/serde";
    "github.com/marekgalovic/tau/cluster";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk"
    log "github.com/Sirupsen/logrus";
)

func main() {
    zkConn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, 1 * time.Second)
    if err != nil {
        log.Fatal(err)
    }

    c, err := cluster.NewCluster(zkConn)
    if err != nil {
        log.Fatal(err)
    }
    if err = c.Register(); err != nil {
        log.Fatal(err)
    }

    datasetsManager, err := tau.NewDatasetsManager(zkConn, storage.NewLocal())
    if err != nil {
        log.Fatal(err)
    }

    datasetsManager.Create("photos", "./examples/data/random_normal.csv", index.NewBtreeIndex(256, "Euclidean", 10, 256))

    log.Info(datasetsManager)

    // dataset := tau.NewDataset("photos", index.NewBtreeIndex(256, "Euclidean", 10, 256), )
    
    // start := time.Now()
    // if err = dataset.Load("./examples/data/random_normal.csv", serde.NewCsv(",")); err != nil {
    //     log.Fatal(err)
    // }
    // log.Info("Data load time", time.Since(start))

    // log.Info(dataset)

    <- utils.InterruptSignal()
}

// func main() {
//     dsm := tau.NewDatasetsManager()
//     err := dsm.Add("photos", index.NewBtreeIndex(256, "Euclidean", 10, 256))
//     if err != nil {
//         log.Fatal(err)
//     }

//     log.Info(dsm.Get("photos"))

//     server := tau.NewServer(dsm)

//     if err = server.Start(); err != nil {
//         log.Fatal(err)
//     }

//     log.Info(server)

//     <- utils.InterruptSignal()
//     server.Stop()
//     log.Info("Done")
// }
