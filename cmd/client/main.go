package main

import (
    "time";

    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/client";

    "google.golang.org/grpc";
    log "github.com/Sirupsen/logrus"
)

func main() {
    client, err := client.New("127.0.0.1:5555", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    datasets, err := client.ListDatasets()
    if err != nil {
        log.Fatal(err)
    }
    for _, dataset := range datasets {
        log.Infof("Name: %s, Partitions: %d, Replicas: %d, Index: %s", dataset.GetName(), dataset.GetNumPartitions(), dataset.GetNumReplicas(), dataset.GetIndex())
    }

    name := "foobar1"
    d := &pb.Dataset {
        Name: name,
        Path: "./examples/data/random_*",
        NumPartitions: 1,
        NumReplicas: 1,
        Index: &pb.Index {
            Size: 256,
            Metric: "Euclidean",
            Options: &pb.Index_Voronoi {
                Voronoi: &pb.VoronoiIndexOptions {
                    SplitFactor: 10,
                    MaxCellItems: 512,
                },
            },
        },
    }

    for i := 0; i < 100; i++ {
        if err := client.CreateDataset(d); err != nil {
            log.Fatal(err)
        }

        <- time.After(10 * time.Millisecond)

        if err := client.DeleteDataset(name); err != nil {
            log.Fatal(err)
        }
    }
}
