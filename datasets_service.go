package tau

import (
    "fmt";
    "sort";
    "context";
    "path/filepath";
    "errors";

    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
)

type datasetsService struct {
    config *Config
    zk *zk.Conn
    storage storage.Storage
}

func newDatasetsService(config *Config, zkConn *zk.Conn, storage storage.Storage) *datasetsService {
    return &datasetsService {
        config: config,
        zk: zkConn,
        storage: storage,
    }
}

func (service *datasetsService) List(req *pb.EmptyRequest, stream pb.DatasetsService_ListServer) error {
    datasetNames, _, err := service.zk.Children(service.zkDatasetsPath())
    if err != nil {
        return err
    }

    for _, datasetName := range datasetNames {
        dataset, err := service.getDataset(datasetName)
        if err != nil {
            return err
        }
        if err := stream.Send(dataset); err != nil {
            return err
        }
    }
    return nil
}

func (service *datasetsService) Get(ctx context.Context, req *pb.GetDatasetRequest) (*pb.Dataset, error) {
    return service.getDataset(req.GetName())
}

func (service *datasetsService) Create(ctx context.Context, req *pb.CreateDatasetRequest) (*pb.EmptyResponse, error) {
    partitions, err := service.computeDatasetPartitions(req.GetDataset())
    if err != nil {
        return nil, err
    }

    return service.CreateWithPartitions(ctx, &pb.CreateDatasetWithPartitionsRequest {
        Dataset: req.GetDataset(),
        Partitions: partitions,
    })
}

func (service *datasetsService) CreateWithPartitions(ctx context.Context, req *pb.CreateDatasetWithPartitionsRequest) (*pb.EmptyResponse, error) {
    dataset := req.GetDataset()
    partitions := req.GetPartitions()

    if dataset == nil {
        return nil, errors.New("No dataset")
    }
    if partitions == nil {
        return nil, errors.New("No partitions")
    }
    if len(partitions) == 0 {
        return nil, errors.New("No partitions")
    }

    datasetData, err := proto.Marshal(dataset)
    if err != nil {
        return nil, err
    }

    requests := []interface{} {
        &zk.CreateRequest{Path: filepath.Join(service.zkDatasetsPath(), dataset.GetName()), Data: datasetData, Flags: int32(0), Acl: zk.WorldACL(zk.PermAll)},
        &zk.CreateRequest{Path: filepath.Join(service.zkDatasetsPath(), dataset.GetName(), "partitions"), Data: nil, Flags: int32(0), Acl: zk.WorldACL(zk.PermAll)},
    }

    for i, partition := range partitions {
        partitionData, err := proto.Marshal(partition)
        if err != nil {
            return nil, err
        }

        requests = append(requests, &zk.CreateRequest{
            Path: filepath.Join(service.zkDatasetsPath(), dataset.GetName(), "partitions", fmt.Sprintf("%d", i)),
            Data: partitionData,
            Flags: int32(0),
            Acl: zk.WorldACL(zk.PermAll),
        })
    }

    if _, err := service.zk.Multi(requests...); err != nil {
        return nil, err
    }
    
    return &pb.EmptyResponse{}, nil
}

func (service *datasetsService) Delete(ctx context.Context, req *pb.DeleteDatasetRequest) (*pb.EmptyResponse, error) {
    if err := utils.ZkRecursiveDelete(service.zk, filepath.Join(service.zkDatasetsPath(), req.GetName())); err != nil {
        return nil, err
    }

    return &pb.EmptyResponse{}, nil
}

func (service *datasetsService) ListPartitions(req *pb.ListPartitionsRequest, stream pb.DatasetsService_ListPartitionsServer) error {
    partitions, _, err := service.zk.Children(filepath.Join(service.zkDatasetsPath(), req.GetDatasetName(), "partitions"))
    if err != nil {
        return err
    }

    for _, partitionId := range partitions {
        partition, err := service.getDatasetPartition(req.GetDatasetName(), partitionId)
        if err != nil {
            return err
        }
        if err := stream.Send(partition); err != nil {
            return err
        }
    }
    return nil
}

func (service *datasetsService) zkDatasetsPath() string {
    return filepath.Join(service.config.Zookeeper.BasePath, "datasets")
}

func (service *datasetsService) getDataset(name string) (*pb.Dataset, error) {
    zkPath := filepath.Join(service.zkDatasetsPath(), name)
    datasetData, _, err := service.zk.Get(zkPath)
    if err != nil {
        return nil, err
    }

    dataset := &pb.Dataset{}
    if err = proto.Unmarshal(datasetData, dataset); err != nil {
        return nil, err
    }
    dataset.ZkPath = zkPath

    return dataset, nil
}

func (service *datasetsService) getDatasetPartition(datasetName, partitionId string) (*pb.DatasetPartition, error) {
    partitionData, _, err := service.zk.Get(filepath.Join(service.zkDatasetsPath(), datasetName, "partitions", partitionId))
    if err != nil {
        return nil, err
    }

    partition := &pb.DatasetPartition{}
    if err = proto.Unmarshal(partitionData, partition); err != nil {
        return nil, err
    }
    return partition, nil
}

func (service *datasetsService) computeDatasetPartitions(dataset *pb.Dataset) ([]*pb.DatasetPartition, error) {
    files, err := service.storage.ListFiles(dataset.GetPath())
    if err != nil {
        return nil, err
    }
    sort.Strings(files)

    numPartitions := int(dataset.GetNumPartitions())
    if len(files) < numPartitions {
        return nil, errors.New("Number of partitions is greater than the number of files")
    }

    partitions := make([]*pb.DatasetPartition, numPartitions)
    numFilesPerPartition := numPartitions / len(files)
    for i := 0; i < numPartitions; i++ {
        lbIdx := i * numFilesPerPartition
        ubIdx := i * numFilesPerPartition + numFilesPerPartition
        if i == numPartitions - 1 {
            ubIdx += len(files) - numPartitions * numFilesPerPartition
        }
        partitions[i] = &pb.DatasetPartition{Id: int32(i), Files: files[lbIdx:ubIdx]}
    }
    return partitions, nil
}
