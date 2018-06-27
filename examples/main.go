package main

import (
    "github.com/marekgalovic/tau";
    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/storage/serde";
    // "github.com/marekgalovic/tau/utils";

    log "github.com/Sirupsen/logrus";
)

func main() {
    nm := tau.NewNodeManager()
    dataset := tau.NewDataset("photos", index.NewBtreeIndex(256, "Euclidean", 10, 256), storage.NewLocal(), nm)
    
    if err := dataset.Load("./examples/data/random_normal.csv", serde.NewCsv(",")); err != nil {
        log.Fatal(err)
    }

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
