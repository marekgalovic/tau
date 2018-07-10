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
    Search(int, []float32) error
    // Datasets
    ListDatasets() ([]*pb.Dataset, error)
    GetDataset(string) (*pb.Dataset, error)
    CreateDataset(*pb.Dataset) error
    DeleteDataset(string) error
}

type client struct {
    conn *grpc.ClientConn
    datasetsService pb.DatasetsServiceClient
}

func New(serverAddr string, opts ...grpc.DialOption) (Client, error) {
    conn, err := grpc.Dial(serverAddr, opts...)
    if err != nil {
        return nil, err
    }

    return &client {
        conn: conn,
        datasetsService: pb.NewDatasetsServiceClient(conn),
    }, nil
}

func (c *client) Close() error {
    return c.conn.Close()
}

func (c *client) Search(k int, query []float32) error {
    return nil
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

func (c *client) DeleteDataset(name string) error {
    _, err := c.datasetsService.Delete(context.Background(), &pb.DeleteDatasetRequest{Name: name})

    return err
}
