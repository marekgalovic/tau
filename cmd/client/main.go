package main

import (
    // "time";

    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/client";

    "google.golang.org/grpc";
    log "github.com/Sirupsen/logrus"
)

func printDatasets(client client.Client) {
    datasets, err := client.ListDatasets()
    if err != nil {
        log.Fatal(err)
    }
    for _, dataset := range datasets {
        log.Infof("Name: %s, Partitions: %d, Replicas: %d, Index: %s", dataset.GetName(), dataset.GetNumPartitions(), dataset.GetNumReplicas(), dataset.GetIndex())
    }
}

func main() {
    client, err := client.New("127.0.0.1:5555", grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    printDatasets(client)

    log.Info(client.DeleteDataset("dataset3"))

    name := "dataset3"
    d := &pb.Dataset {
        Name: name,
        // Path: "./examples/data/random_*",
        // NumPartitions: 1,
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

    p := []*pb.DatasetPartition {
        &pb.DatasetPartition{Id: 1, Files: []string{"./examples/data/random_normal.csv"}},
        &pb.DatasetPartition{Id: 2, Files: []string{"./examples/data/random_normal.csv"}},
        &pb.DatasetPartition{Id: 3, Files: []string{"./examples/data/random_normal.csv"}},
        &pb.DatasetPartition{Id: 4, Files: []string{"./examples/data/random_normal.csv"}},
        &pb.DatasetPartition{Id: 5, Files: []string{"./examples/data/random_normal.csv"}},
    }

    log.Info(p, d)
    // if err := client.CreateDatasetWithPartitions(d, p); err != nil {
    //     log.Fatal(err)
    // }

    // printDatasets(client)


    // for i := 0; i < 100; i++ {
    //     if err := client.CreateDataset(d); err != nil {
    //         log.Fatal(err)
    //     }

    //     <- time.After(10 * time.Millisecond)

    //     if err := client.DeleteDataset(name); err != nil {
    //         log.Fatal(err)
    //     }
    // }
}
