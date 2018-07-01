package main

import (
    "github.com/marekgalovic/tau/storage"

    log "github.com/Sirupsen/logrus"
)

func main() {
    gcs, err := storage.NewGCS()
    if err != nil {
        log.Fatal(err)
    }

    log.Info(gcs.Exists("gs://gmv-forecast/data/darn_grouped_v2/feature_stats.json"))

    log.Info(gcs.ListFiles("gs://gmv-forecast/data/weekly_grouped_v3/train"))
}
