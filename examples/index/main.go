package main

import (
    "os";

    "github.com/marekgalovic/tau/index";

    log "github.com/Sirupsen/logrus";
)

func main() {
    log.Info("Tau")

    btree := index.NewBtreeIndex(256, "Euclidean", 5, 1024)

    f, err := os.Create("/tmp/taubtreeindex")
    if err != nil {
        log.Fatal(err)
    }

    log.Info(btree.Save(f))

    f.Close()

    f, err = os.Open("/tmp/taubtreeindex")
    if err != nil {
        log.Fatal(err)
    }

    log.Info(btree.Load(f))

    log.Info(btree)
}
