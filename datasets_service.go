package tau

import (
    "fmt";
    "sort";
    "context";
    "path/filepath";
    "errors";

    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/dataset";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";

    log "github.com/Sirupsen/logrus"
)

type datasetsService struct {
    config *Config
    zk utils.Zookeeper
    storage storage.Storage
}

var (
    ErrDatasetNotFound error = errors.New("Dataset not found")
    ErrDatasetAlreadyExists error = errors.New("Dataset already exists")    
)

func newDatasetsService(config *Config, zk utils.Zookeeper, storage storage.Storage) *datasetsService {
    return &datasetsService {
        config: config,
        zk: zk,
        storage: storage,
    }
}

func (service *datasetsService) List(req *pb.EmptyRequest, stream pb.DatasetsService_ListServer) error {
    datasetNames, err := service.zk.Children(dataset.ZkDatasetsPath)
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
    datasetProto := req.GetDataset()
    partitionProtos := req.GetPartitions()

    if datasetProto == nil {
        return nil, errors.New("No dataset")
    }
    if partitionProtos == nil {
        return nil, errors.New("No partitions")
    }
    if len(partitionProtos) == 0 {
        return nil, errors.New("No partitions")
    }

    datasetData, err := proto.Marshal(datasetProto)
    if err != nil {
        return nil, err
    }

    requests := []interface{} {
        &zk.CreateRequest{Path: filepath.Join(dataset.ZkDatasetsPath, datasetProto.GetName()), Data: datasetData, Flags: int32(0), Acl: zk.WorldACL(zk.PermAll)},
        &zk.CreateRequest{Path: filepath.Join(dataset.ZkDatasetsPath, datasetProto.GetName(), "partitions"), Data: nil, Flags: int32(0), Acl: zk.WorldACL(zk.PermAll)},
    }

    for _, partition := range partitionProtos {
        log.Infof("Partition: %d, files: %s", partition.GetId(), partition.GetFiles())

        partitionData, err := proto.Marshal(partition)
        if err != nil {
            return nil, err
        }

        requests = append(requests, &zk.CreateRequest{
            Path: filepath.Join(dataset.ZkDatasetsPath, datasetProto.GetName(), "partitions", fmt.Sprintf("%d", partition.Id)),
            Data: partitionData,
            Flags: int32(0),
            Acl: zk.WorldACL(zk.PermAll),
        })
    }

    err = service.zk.Multi(requests...)
    if err == zk.ErrNodeExists {
        return nil, ErrDatasetAlreadyExists
    }
    if err != nil {
        return nil, err
    }
    
    return &pb.EmptyResponse{}, nil
}

func (service *datasetsService) Delete(ctx context.Context, req *pb.DeleteDatasetRequest) (*pb.EmptyResponse, error) {
    err := service.zk.DeleteRecursive(filepath.Join(dataset.ZkDatasetsPath, req.GetName()));
    if err == zk.ErrNoNode {
        return nil, ErrDatasetNotFound
    }
    if err != nil {
        return nil, err
    }

    return &pb.EmptyResponse{}, nil
}

func (service *datasetsService) ListPartitions(req *pb.ListPartitionsRequest, stream pb.DatasetsService_ListPartitionsServer) error {
    partitions, err := service.zk.Children(filepath.Join(dataset.ZkDatasetsPath, req.GetDatasetName(), "partitions"))
    if err == zk.ErrNoNode {
        return ErrDatasetNotFound
    }
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

func (service *datasetsService) getDataset(name string) (*pb.Dataset, error) {
    zkPath := filepath.Join(dataset.ZkDatasetsPath, name)
    datasetData, err := service.zk.Get(zkPath)
    if err == zk.ErrNoNode {
        return nil, ErrDatasetNotFound
    }
    if err != nil {
        return nil, err
    }

    dataset := &pb.Dataset{}
    if err = proto.Unmarshal(datasetData, dataset); err != nil {
        return nil, err
    }

    return dataset, nil
}

func (service *datasetsService) getDatasetPartition(datasetName, partitionId string) (*pb.DatasetPartition, error) {
    partitionData, err := service.zk.Get(filepath.Join(dataset.ZkDatasetsPath, datasetName, "partitions", partitionId))
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

    log.Infof("Num partitions: %d", numPartitions)

    partitions := make([]*pb.DatasetPartition, numPartitions)
    numFilesPerPartition := len(files) / numPartitions
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
