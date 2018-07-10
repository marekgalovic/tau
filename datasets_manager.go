package tau

import (
    "fmt";
    "sort";
    "errors";
    "context";
    "path/filepath";

    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
    log "github.com/Sirupsen/logrus";
)

var (
    DatasetAlreadyExistsErr = errors.New("Dataset already exists")
    NoNodesAvailableErr = errors.New("No nodes available")
)

const (
    EventDatasetCreated int32 = 1
    EventDatasetDeleted int32 = 2
)

type DatasetsManager interface {
    ListDatasets() ([]Dataset, error)
    GetDataset(string) (Dataset, error)
    CreateDataset(*pb.Dataset) error
    DeleteDataset(string) error
    DatasetExists(string) (bool, error)
}

type datasetsManager struct {
    ctx context.Context
    config *Config
    zk *zk.Conn
    cluster Cluster
    storage storage.Storage

    datasets map[string]Dataset
    datasetChangesNotifications utils.Broadcaster
}

type DatasetsChangedNotification struct {
    Event int32
    Dataset Dataset
}

func NewDatasetsManager(ctx context.Context, config *Config, zkConn *zk.Conn, cluster Cluster, storage storage.Storage) (DatasetsManager, error) {
    dm := &datasetsManager {
        ctx: ctx,
        config: config,
        zk: zkConn,
        cluster: cluster,
        storage: storage,
        datasets: make(map[string]Dataset),
        datasetChangesNotifications: utils.NewThreadSafeBroadcast(),
    }
    if err := dm.bootstrapZk(); err != nil {
        return nil, err
    }

    go dm.watchDatasets()
    go dm.run()

    return dm, nil
}

func (dm *datasetsManager) bootstrapZk() error {
    paths := []string{
        dm.zkDatasetsPath(),
    }

    for _, path := range paths {
        err := utils.ZkCreatePath(dm.zk, path, nil, int32(0), zk.WorldACL(zk.PermAll))
        if err == zk.ErrNodeExists {
            continue
        }
        if err != nil {
            return err
        }  
    }
    return nil
}

func (dm *datasetsManager) watchDatasets() {
    for {
        datasets, _, event, err := dm.zk.ChildrenW(dm.zkDatasetsPath())
        if err != nil {
            panic(err)
        }
        if err := dm.updateDatasets(datasets); err != nil {
            panic(err)
        }

        select {
        case <- event:
        case <- dm.ctx.Done():
            return
        }
    }
}

func (dm *datasetsManager) updateDatasets(datasets []string) error {
    updatedDatasets := utils.NewSet()
    for _, datasetName := range datasets {
        updatedDatasets.Add(datasetName)

        if _, exists := dm.datasets[datasetName]; !exists {
            var err error
            if dm.datasets[datasetName], err = dm.GetDataset(datasetName); err != nil {
                return err
            }
            dm.datasetChangesNotifications.Send(&DatasetsChangedNotification{
                Event: EventDatasetCreated,
                Dataset: dm.datasets[datasetName],
            })
        }
    }

    for datasetName, dataset := range dm.datasets {
        if !updatedDatasets.Contains(datasetName) {
            delete(dm.datasets, datasetName)
            dm.datasetChangesNotifications.Send(&DatasetsChangedNotification{
                Event: EventDatasetDeleted,
                Dataset: dataset,
            })
        }
    }
    return nil
}

func (dm *datasetsManager) run() {
    nodeNotifications := dm.cluster.NodeChanges()
    datasetNotifications := dm.datasetChangesNotifications.Listen()

    for {
        select {
        case n := <- datasetNotifications:
            notification := n.(*DatasetsChangedNotification)

            switch notification.Event {
            case EventDatasetCreated:
                log.Infof("DM dataset created: %s", notification.Dataset.Meta().GetName())
            }
        case n := <- nodeNotifications:
            notification := n.(*NodesChangedNotification)

            switch notification.Event {
            case EventNodeCreated:
                log.Infof("DM node created: %s", notification.Node.Meta().GetUuid())
            case EventNodeDeleted:
                log.Infof("DM node deleted: %s", notification.Node.Meta().GetUuid())
            }
        case <- dm.ctx.Done():
            return
        }
    }
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
    zkPath := filepath.Join(dm.zkDatasetsPath(), name)
    datasetData, _, err := dm.zk.Get(zkPath)
    if err != nil {
        return nil, err
    }

    datasetMeta := &pb.Dataset{}
    if err = proto.Unmarshal(datasetData, datasetMeta); err != nil {
        return nil, err
    }
    datasetMeta.ZkPath = zkPath

    return &dataset {
        meta: datasetMeta,
    }, nil
}

func (dm *datasetsManager) CreateDataset(dataset *pb.Dataset) error {
    partitions, err := dm.getDatasetPartitions(dataset)
    if err != nil {
        return err
    }

    datasetMetadata, err := proto.Marshal(dataset)
    if err != nil {
        return err
    }

    requests := []interface{}{
        &zk.CreateRequest{Path: filepath.Join(dm.zkDatasetsPath(), dataset.GetName()), Data: datasetMetadata, Flags: int32(0), Acl: zk.WorldACL(zk.PermAll)},
        &zk.CreateRequest{Path: filepath.Join(dm.zkDatasetsPath(), dataset.GetName(), "partitions"), Data: nil, Flags: int32(0), Acl: zk.WorldACL(zk.PermAll)},
    }

    for i, partition := range partitions {
        partitionData, err := proto.Marshal(partition)
        if err != nil {
            return err
        }

        requests = append(requests, &zk.CreateRequest{
            Path: filepath.Join(dm.zkDatasetsPath(), dataset.GetName(), "partitions", fmt.Sprintf("%d", i)),
            Data: partitionData,
            Flags: int32(0),
            Acl: zk.WorldACL(zk.PermAll),
        })
    }

    _, err = dm.zk.Multi(requests...)
    if err == zk.ErrNodeExists {
        return DatasetAlreadyExistsErr
    }

    return err
}

func (dm *datasetsManager) DeleteDataset(name string) error {
    return utils.ZkRecursiveDelete(dm.zk, filepath.Join(dm.zkDatasetsPath(), name))
}

func (dm *datasetsManager) DatasetExists(name string) (bool, error) {
    exists, _, err := dm.zk.Exists(filepath.Join(dm.zkDatasetsPath(), name))

    return exists, err
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
        partitions[i] = &pb.DatasetPartition{Id: int32(i), Files: files[lbIdx:ubIdx]}
    }
    return partitions, nil
}
