package main

import (
    "time";
    
    "github.com/marekgalovic/tau";
    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/storage/serde";
    // "github.com/marekgalovic/tau/utils";

    log "github.com/Sirupsen/logrus";
)

func main() {
    cluster, err := tau.NewCluster([]string{"127.0.0.1:2181"}, 1 * time.Second)
    if err != nil {
        log.Fatal(err)
    }
    if err = cluster.Register(); err != nil {
        log.Fatal(err)
    }

    dataset := tau.NewDataset("photos", index.NewBtreeIndex(256, "Euclidean", 10, 256), storage.NewLocal(), cluster)
    
    start := time.Now()
    if err = dataset.Load("./examples/data/random_normal.csv", serde.NewCsv(",")); err != nil {
        log.Fatal(err)
    }
    log.Info("Data load time", time.Since(start))

    log.Info(dataset)
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
