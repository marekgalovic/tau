package tau

import (
    "context";
    "path/filepath";

    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
)

type datasetsService struct {
    config *Config
    zk *zk.Conn
}

func newDatasetsService(config *Config, zkConn *zk.Conn) *datasetsService {
    return &datasetsService {
        config: config,
        zk: zkConn,
    }
}

func (service *datasetsService) List(req *pb.EmptyRequest, stream pb.DatasetsService_ListServer) error {
    datasetNames, _, err := service.zk.Children(service.zkDatasetsPath())
    if err != nil {
        return err
    }

    for _, datasetName := range datasetNames {
        dataset, err := service.get(datasetName)
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
    return service.get(req.GetName())
}

func (service *datasetsService) Create(ctx context.Context, req *pb.CreateDatasetRequest) (*pb.EmptyResponse, error) {
    dataset, err := proto.Marshal(req.GetDataset())
    if err != nil {
        return nil, err
    }

    if _, err := service.zk.Create(filepath.Join(service.zkDatasetsPath(), req.GetDataset().GetName()), dataset, int32(0), zk.WorldACL(zk.PermAll)); err != nil {
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

func (service *datasetsService) get(name string) (*pb.Dataset, error) {
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

func (service *datasetsService) zkDatasetsPath() string {
    return filepath.Join(service.config.Zookeeper.BasePath, "datasets")
}

// func (dm *datasetsManager) getDatasetPartitions(dataset *pb.Dataset) ([]*pb.DatasetPartition, error) {
//     files, err := dm.storage.ListFiles(dataset.GetPath())
//     if err != nil {
//         return nil, err
//     }
//     sort.Strings(files)

//     numPartitions := int(dataset.GetNumPartitions())
//     if len(files) < numPartitions {
//         log.Warn("Number of files is less than the number of partitions")
//         numPartitions = len(files)
//     }

//     partitions := make([]*pb.DatasetPartition, numPartitions)
//     numFilesPerPartition := numPartitions / len(files)
//     for i := 0; i < numPartitions; i++ {
//         lbIdx := i * numFilesPerPartition
//         ubIdx := i * numFilesPerPartition + numFilesPerPartition
//         if i == numPartitions - 1 {
//             ubIdx += len(files) - numPartitions * numFilesPerPartition
//         }
//         partitions[i] = &pb.DatasetPartition{Id: int32(i), Files: files[lbIdx:ubIdx]}
//     }
//     return partitions, nil
// }
