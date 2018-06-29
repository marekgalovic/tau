package tau

import (
    "fmt";

    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";

    "github.com/samuel/go-zookeeper/zk";
)

type DatasetsManager interface {
    Create(string, string, index.Index) error
    Get(string) (index.Index, error)
}

type datasetsManager struct {
    zk *zk.Conn
    storage storage.Storage
}

func NewDatasetsManager(zkConn *zk.Conn, storage storage.Storage) DatasetsManager {
    return &datasetsManager {
        zk: zkConn,
        storage: storage,
    }
}

func (dm *datasetsManager) Create(name, path string, index index.Index) error {
    d := NewDataset(name, path, index, dm.storage)

    fmt.Println(d.meta)
    return nil
}

func (dm *datasetsManager) Get(name string) (index.Index, error) {
    return nil, nil
}
