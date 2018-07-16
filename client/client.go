package client

import (
    "io";
    "context";
    
    pb "github.com/marekgalovic/tau/protobuf";

    "google.golang.org/grpc"
)

type Client interface {
    Close() error
    // Search
    Search(string, int, []float32) ([]*pb.SearchResultItem, error)
    // Datasets
    ListDatasets() ([]*pb.Dataset, error)
    GetDataset(string) (*pb.Dataset, error)
    CreateDataset(*pb.Dataset) error
    CreateDatasetWithPartitions(*pb.Dataset, []*pb.DatasetPartition) error
    DeleteDataset(string) error
}

type client struct {
    conn *grpc.ClientConn
    searchService pb.SearchServiceClient
    datasetsService pb.DatasetsServiceClient
}

func New(serverAddr string, opts ...grpc.DialOption) (Client, error) {
    conn, err := grpc.Dial(serverAddr, opts...)
    if err != nil {
        return nil, err
    }

    return &client {
        conn: conn,
        searchService: pb.NewSearchServiceClient(conn),
        datasetsService: pb.NewDatasetsServiceClient(conn),
    }, nil
}

func (c *client) Close() error {
    return c.conn.Close()
}

func (c *client) Search(dataset string, k int, query []float32) ([]*pb.SearchResultItem, error) {
    stream, err := c.searchService.Search(context.Background(), &pb.SearchRequest{
        DatasetName: dataset,
        K: int32(k),
        Query: query,
    })
    if err != nil {
        return nil, err
    }

    results := make([]*pb.SearchResultItem, 0)
    for {
        item, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, err
        }
        results = append(results, item)
    }
    return results, nil
}

func (c *client) ListDatasets() ([]*pb.Dataset, error) {
    stream, err := c.datasetsService.List(context.Background(), &pb.EmptyRequest{})
    if err != nil {
        return nil, err
    }

    datasets := make([]*pb.Dataset, 0)
    for {
        dataset, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, err
        }
        datasets = append(datasets, dataset)
    }
    return datasets, nil
}

func (c *client) GetDataset(name string) (*pb.Dataset, error) {
    return c.datasetsService.Get(context.Background(), &pb.GetDatasetRequest{Name: name})
}

func (c *client) CreateDataset(dataset *pb.Dataset) error {
    _, err := c.datasetsService.Create(context.Background(), &pb.CreateDatasetRequest{Dataset: dataset})

    return err
}

func (c *client) CreateDatasetWithPartitions(dataset *pb.Dataset, partitions []*pb.DatasetPartition) error {
    _, err := c.datasetsService.CreateWithPartitions(context.Background(), &pb.CreateDatasetWithPartitionsRequest{
        Dataset: dataset,
        Partitions: partitions,
    })

    return err
}

func (c *client) DeleteDataset(name string) error {
    _, err := c.datasetsService.Delete(context.Background(), &pb.DeleteDatasetRequest{Name: name})

    return err
}
