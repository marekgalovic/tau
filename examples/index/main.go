package main

import (
    "os";
    "time";

    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/math";

    log "github.com/Sirupsen/logrus";
)

func main() {
    log.Info("Tau")

    btree := index.NewBtreeIndex(256, "Euclidean", 5, 1024)
    for i := 0; i < 1000000; i++ {
        btree.Add(int64(i), math.RandomUniformVector(256))
    }

    log.Info("Start build")

    start := time.Now()
    btree.Build()
    log.Info("Build time:", time.Since(start))

    f, err := os.Create("/tmp/taubtreeindex")
    if err != nil {
        log.Fatal(err)
    }

    start = time.Now()
    log.Info(btree.Save(f))
    log.Info("Save time:", time.Since(start))

    f.Close()

    f, err = os.Open("/tmp/taubtreeindex")
    if err != nil {
        log.Fatal(err)
    }

    start = time.Now()
    log.Info(btree.Load(f))
    log.Info("Load time:", time.Since(start))
}
