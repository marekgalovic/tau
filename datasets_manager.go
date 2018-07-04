package tau

import (
    "fmt";
    "sort";
    "errors";
    "context";
    "path/filepath";

    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
    log "github.com/Sirupsen/logrus";
)

var (
    DatasetAlreadyExistsErr = errors.New("Dataset already exists")
    NoNodesAvailableErr = errors.New("No nodes available")
)

type DatasetsManager interface {
    BuildPartition(context.Context, *pb.BuildPartitionRequest) (*pb.EmptyResponse, error)
    ListDatasets() ([]Dataset, error)
    DatasetExists(string) (bool, error)
    GetDataset(string) (Dataset, error)
    CreateDataset(*pb.Dataset) error
}

type datasetsManager struct {
    config *Config
    zk *zk.Conn
    cluster Cluster
    storage storage.Storage

    localPartitions map[string]Dataset
}

func NewDatasetsManager(config *Config, zkConn *zk.Conn, cluster Cluster, storage storage.Storage) (DatasetsManager, error) {
    dm := &datasetsManager {
        config: config,
        zk: zkConn,
        cluster: cluster,
        storage: storage,
        localPartitions: make(map[string]Dataset),
    }
    if err := dm.bootstrapZk(); err != nil {
        return nil, err
    }

    go dm.master()

    return dm, nil
}

func (dm *datasetsManager) bootstrapZk() error {
    paths := []string{
        dm.config.Zookeeper.BasePath,
        dm.zkDatasetsPath(),
    }

    for _, path := range paths {
        exists, _, err := dm.zk.Exists(path)
        if err != nil {
            return err
        }
        if !exists {
            _, err = dm.zk.Create(path, nil, int32(0), zk.WorldACL(zk.PermAll))
            if err != nil {
                return err
            }
        }   
    }
    return nil
}

func (dm *datasetsManager) master() {
    <-dm.cluster.NotifyWhenMaster()
    log.Info("Datasets manager master")
}

func (dm *datasetsManager) BuildPartition(ctx context.Context, req *pb.BuildPartitionRequest) (*pb.EmptyResponse, error) {
    if _, exists := dm.localPartitions[req.Dataset.Name]; exists {
        return &pb.EmptyResponse{}, nil
    }

    go func() {
        dataset := newDatasetFromProto(req.Dataset, dm.storage)
        if err := dataset.Load(req.Partition.Files); err != nil {
            fmt.Println(err)
        }

        dm.localPartitions[dataset.Meta().Name] = dataset
        log.Infof("Built local partition: %s", dataset.Meta().Name)
    }()

    return &pb.EmptyResponse{}, nil
}

func (dm *datasetsManager) ListDatasets() ([]Dataset, error) {
    datasetNames, _, err := dm.zk.Children(dm.zkDatasetsPath())
    if err != nil {
        return nil, err
    }

    datasets := make([]Dataset, len(datasetNames))
    for i, datasetName := range datasetNames {
        if datasets[i], err = dm.GetDataset(datasetName); err != nil {
            return nil, err
        }
    }
    return datasets, nil
}

func (dm *datasetsManager) GetDataset(name string) (Dataset, error) {
    datasetData, _, err := dm.zk.Get(filepath.Join(dm.zkDatasetsPath(), name))
    if err != nil {
        return nil, err
    }

    datasetMeta := &pb.Dataset{}
    if err = proto.Unmarshal(datasetData, datasetMeta); err != nil {
        return nil, err
    }

    return &dataset {
        meta: datasetMeta,
    }, nil
}

func (dm *datasetsManager) DatasetExists(name string) (bool, error) {
    exists, _, err := dm.zk.Exists(filepath.Join(dm.zkDatasetsPath(), name))

    return exists, err
}

func (dm *datasetsManager) CreateDataset(dataset *pb.Dataset) error {
    exists, err := dm.DatasetExists(dataset.GetName())
    if err != nil {
        return err
    }
    if exists {
        return DatasetAlreadyExistsErr
    }

    partitions, err := dm.getDatasetPartitions(dataset)
    if err != nil {
        return err
    }

    datasetMetadata, err := proto.Marshal(dataset)
    if err != nil {
        return err
    }
    if _, err = dm.zk.Create(filepath.Join(dm.zkDatasetsPath(), dataset.GetName()), datasetMetadata, int32(0), zk.WorldACL(zk.PermAll)); err != nil {
        return err
    }
    if _, err := dm.zk.Create(filepath.Join(dm.zkDatasetsPath(), dataset.GetName(), "partitions"), nil, int32(0), zk.WorldACL(zk.PermAll)); err != nil {
        return err
    }

    for i, partition := range partitions {
        partitionData, err := proto.Marshal(partition)
        if err != nil {
            return err
        }
        if _, err := dm.zk.Create(filepath.Join(dm.zkDatasetsPath(), dataset.GetName(), "partitions", fmt.Sprintf("%d", i)), partitionData, int32(0), zk.WorldACL(zk.PermAll)); err != nil {
            return err
        }
    }

    return nil
}

func (dm *datasetsManager) zkDatasetsPath() string {
    return filepath.Join(dm.config.Zookeeper.BasePath, "datasets")
}

func (dm *datasetsManager) getDatasetPartitions(dataset *pb.Dataset) ([]*pb.DatasetPartition, error) {
    files, err := dm.storage.ListFiles(dataset.GetPath())
    if err != nil {
        return nil, err
    }
    sort.Strings(files)

    numPartitions := int(dataset.GetNumPartitions())
    if len(files) < numPartitions {
        log.Warn("Number of files is less than the number of partitions")
        numPartitions = len(files)
    }

    partitions := make([]*pb.DatasetPartition, numPartitions)
    numFilesPerPartition := numPartitions / len(files)
    for i := 0; i < numPartitions; i++ {
        lbIdx := i * numFilesPerPartition
        ubIdx := i * numFilesPerPartition + numFilesPerPartition
        if i == numPartitions - 1 {
            ubIdx += len(files) - numPartitions * numFilesPerPartition
        }
        partitions[i] = &pb.DatasetPartition{Files: files[lbIdx:ubIdx]}
    }
    return partitions, nil
}
