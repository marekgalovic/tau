package tau

import (
    "fmt";
    "context";
    "path/filepath";

    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
)

type DatasetsManager interface {
    BuildPartition(context.Context, *pb.BuildPartitionRequest) (*pb.EmptyResponse, error)
    ListDatasets() ([]Dataset, error)
    GetDataset(string) (Dataset, error)
    CreateDataset(string, string, index.Index) error
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
    }
    if err := dm.bootstrapZk(); err != nil {
        return nil, err
    }
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

func (dm *datasetsManager) BuildPartition(ctx context.Context, req *pb.BuildPartitionRequest) (*pb.EmptyResponse, error) {
    return nil, nil
}

func (dm *datasetsManager) ListDatasets() ([]Dataset, error) {
    datasetNames, _, err := dm.zk.Children(dm.zkDatasetsPath())
    if err != nil {
        return nil, err
    }

    datasets := make([]Dataset, len(datasetNames))
    for i, datasetName := range datasetNames {
        var err error
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

func (dm *datasetsManager) CreateDataset(name, path string, index index.Index) error {
    dataset := NewDataset(name, path, index, dm.storage)

    datasetMetadata, err := proto.Marshal(dataset.Meta())
    if err != nil {
        return err
    }

    _, err = dm.zk.Create(filepath.Join(dm.zkDatasetsPath(), name), datasetMetadata, int32(0), zk.WorldACL(zk.PermAll))
    if err != nil {
        return err
    }

    files, err := dm.storage.ListFiles(path)
    if err != nil {
        return err
    }

    fmt.Println(files)

    return nil
}

func (dm *datasetsManager) zkDatasetsPath() string {
    return filepath.Join(dm.config.Zookeeper.BasePath, "datasets")
}
