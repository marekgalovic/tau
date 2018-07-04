package tau

import (
    "fmt";
    "errors";
    "context";
    "path/filepath";

    "github.com/marekgalovic/tau/index";
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
    Exists(string) (bool, error)
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
        localPartitions: make(map[string]Dataset),
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
    if _, exists := dm.localPartitions[req.Dataset.Name]; exists {
        return &pb.EmptyResponse{}, nil
    }

    go func() {
        dataset := NewDatasetFromProto(req.Dataset, dm.storage)
        if err := dataset.Load(req.Files); err != nil {
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

func (dm *datasetsManager) Exists(name string) (bool, error) {
    exists, _, err := dm.zk.Exists(filepath.Join(dm.zkDatasetsPath(), name))

    return exists, err
}

func (dm *datasetsManager) CreateDataset(name, path string, index index.Index) error {
    exists, err := dm.Exists(name)
    if err != nil {
        return err
    }
    if exists {
        return DatasetAlreadyExistsErr
    }

    dataset := NewDataset(name, path, index, dm.storage)
    datasetMetadata, err := proto.Marshal(dataset.Meta())
    if err != nil {
        return err
    }
    _, err = dm.zk.Create(filepath.Join(dm.zkDatasetsPath(), name), datasetMetadata, int32(0), zk.WorldACL(zk.PermAll))
    if err != nil {
        return err
    }

    partitions, err := dm.getPartitions(path)
    if err != nil {
        return err
    }

    for node, files := range partitions {
        conn, err := node.Conn()
        if err != nil {
            return err
        }
        if _, err := pb.NewDatasetsManagerClient(conn).BuildPartition(context.Background(), &pb.BuildPartitionRequest {
            Dataset: dataset.Meta(),
            Files: files,
        }); err != nil {
            return err
        }
    }

    return nil
}

func (dm *datasetsManager) zkDatasetsPath() string {
    return filepath.Join(dm.config.Zookeeper.BasePath, "datasets")
}

func (dm *datasetsManager) getPartitions(path string) (map[Node][]string, error) {
    files, err := dm.storage.ListFiles(path)
    if err != nil {
        return nil, err
    }
    nodes, err := dm.cluster.ListNodes()
    if err != nil {
        return nil, err
    }

    if len(nodes) == 0 {
        return nil, NoNodesAvailableErr
    }

    numFilesPerNode := int(float32(len(files)) / float32(len(nodes)))
    if numFilesPerNode < 2 {
        numFilesPerNode = 2
    }

    partitions := make(map[Node][]string, 0)
    nodeIdx := 0
    for i, file := range files {
        if _, exists := partitions[nodes[nodeIdx]]; !exists {
            partitions[nodes[nodeIdx]] = make([]string, 0)
        }
        partitions[nodes[nodeIdx]] = append(partitions[nodes[nodeIdx]], file)

        if (i > 0) && (i % numFilesPerNode == 0) && (nodeIdx + 1 > len(nodes) - 1) {
            nodeIdx++
        }
    }
    return partitions, nil
}
