package tau

import (
    "fmt";
    "path/filepath";

    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/cluster";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
)

type DatasetsManager interface {
    Create(string, string, index.Index) error
    Get(string) (index.Index, error)
}

type datasetsManager struct {
    zk *zk.Conn
    storage storage.Storage
}

func NewDatasetsManager(zkConn *zk.Conn, storage storage.Storage) (DatasetsManager, error) {
    dm := &datasetsManager {
        zk: zkConn,
        storage: storage,
    }
    if err := dm.bootstrapZk(); err != nil {
        return nil, err
    }
    return dm, nil
}

func (dm *datasetsManager) bootstrapZk() error {
    paths := []string{
        cluster.ZkBasePath,
        filepath.Join(cluster.ZkBasePath, "datasets"),
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

func (dm *datasetsManager) Create(name, path string, index index.Index) error {
    dataset := NewDataset(name, path, index, dm.storage)

    datasetMetadata, err := proto.Marshal(dataset.Meta())
    if err != nil {
        return err
    }

    _, err = dm.zk.Create(filepath.Join(cluster.ZkBasePath, "datasets", name), datasetMetadata, int32(0), zk.WorldACL(zk.PermAll))

    fmt.Println(dataset.Meta())
    return err
}

func (dm *datasetsManager) Get(name string) (index.Index, error) {
    return nil, nil
}
