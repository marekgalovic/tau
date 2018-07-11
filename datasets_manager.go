package tau

import (
    // "errors";
    "fmt";
    "context";
    "path/filepath";
    "sync";

    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
    log "github.com/Sirupsen/logrus";
)

const (
    EventDatasetCreated int32 = 1
    EventDatasetDeleted int32 = 2
)

type DatasetsManager interface {
    GetDataset(string) (Dataset, error)
    Run() error
}

type datasetsManager struct {
    ctx context.Context
    config *Config
    zk *zk.Conn
    cluster Cluster
    storage storage.Storage

    datasets map[string]Dataset
    datasetsMutex *sync.RWMutex
    datasetChangesNotifications utils.Broadcast

    localDatasets map[string][]string
    localDatasetsMutex *sync.RWMutex
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
        datasetsMutex: &sync.RWMutex{},
        datasetChangesNotifications: utils.NewThreadSafeBroadcast(),

        localDatasets: make(map[string][]string),
        localDatasetsMutex: &sync.RWMutex{},
    }
    if err := dm.bootstrapZk(); err != nil {
        return nil, err
    }

    return dm, nil
}

func (dm *datasetsManager) addDataset(dataset Dataset) {
    defer dm.datasetsMutex.Unlock()
    dm.datasetsMutex.Lock()

    dm.datasets[dataset.Meta().GetName()] = dataset
}

func (dm *datasetsManager) deleteDataset(name string) {
    defer dm.datasetsMutex.RUnlock()
    dm.datasetsMutex.RLock()

    delete(dm.datasets, name)
}

func (dm *datasetsManager) addLocalDataset(name string, partitions []string) {
    defer dm.localDatasetsMutex.Unlock()
    dm.localDatasetsMutex.Lock()

    dm.localDatasets[name] = partitions
}

func (dm *datasetsManager) deleteLocalDataset(name string) {
    defer dm.localDatasetsMutex.RUnlock()
    dm.localDatasetsMutex.RLock()

    delete(dm.localDatasets, name)
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

func (dm *datasetsManager) bootstrapLocalDatasets() error {
    datasetsWithPartitions, err := dm.ListDatasetsWithPartitions()
    if err != nil {
        return err
    }

    for dataset, partitions := range datasetsWithPartitions {
        dm.addDataset(dataset)

        ownedPartitions := make([]string, 0)
        for _, partitionId := range partitions {
            shouldOwn, err := dm.shouldOwn(dataset.Meta().GetName(), partitionId)
            if err != nil {
                return err
            }
            if shouldOwn {
                ownedPartitions = append(ownedPartitions, partitionId)
            }
        }
        if len(ownedPartitions) > 0 {
            go dm.buildDatasetPartitions(dataset, ownedPartitions)
        }
    }
    return nil
}

func (dm *datasetsManager) Run() error {
    if err := dm.bootstrapLocalDatasets(); err != nil {
        return err
    }

    go dm.watchDatasets()
    go dm.run()

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
            dataset, err := dm.GetDataset(datasetName)
            if err != nil {
                return err
            }
            dm.addDataset(dataset)

            dm.datasetChangesNotifications.Send(&DatasetsChangedNotification{
                Event: EventDatasetCreated,
                Dataset: dataset,
            })
        }
    }

    for datasetName, dataset := range dm.datasets {
        if !updatedDatasets.Contains(datasetName) {
            dm.deleteDataset(datasetName)
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
    datasetNotifications := dm.datasetChangesNotifications.Listen(10)

    for {
        select {
        case n := <- nodeNotifications:
            notification := n.(*NodesChangedNotification)

            switch notification.Event {
            case EventNodeCreated:
                go dm.nodeCreated(notification.Node)
            case EventNodeDeleted:
                go dm.nodeDeleted(notification.Node)
            }
        case n := <- datasetNotifications:
            notification := n.(*DatasetsChangedNotification)

            switch notification.Event {
            case EventDatasetCreated:
                go dm.datasetCreated(notification.Dataset)
            case EventDatasetDeleted:
                go dm.datasetDeleted(notification.Dataset)
            }
        case <- dm.ctx.Done():
            return
        }
    }
}

func (dm *datasetsManager) datasetCreated(dataset Dataset) {
    partitions, err := dm.listDatasetPartitions(dataset.Meta().GetName())
    if err != nil {
        panic(err)
    }

    ownedPartitions := make([]string, 0)
    for _, partitionId := range partitions {
        shouldOwn, err := dm.shouldOwn(dataset.Meta().GetName(), partitionId)
        if err != nil {
            panic(err)
        }
        if shouldOwn {
            ownedPartitions = append(ownedPartitions, partitionId)
        }
    }
    if len(ownedPartitions) > 0 {
        dm.buildDatasetPartitions(dataset, ownedPartitions)
    }
}

func (dm *datasetsManager) datasetDeleted(dataset Dataset) {
    if _, exists := dm.localDatasets[dataset.Meta().GetName()]; exists {
        log.Infof("Delete local: %s", dataset.Meta().GetName())
        dm.deleteLocalDataset(dataset.Meta().GetName())
    }
}

func (dm *datasetsManager) nodeCreated(node Node) {
    for datasetName, _ := range dm.localDatasets {
        partitions, err := dm.listDatasetPartitions(datasetName)
        if err != nil {
            panic(err)
        }

        releasedPartitions := make([]string, 0)
        for _, partitionId := range partitions {
            shouldOwn, err := dm.shouldOwn(datasetName, partitionId)
            if err != nil {
                panic(err)
            }
            if !shouldOwn {
                releasedPartitions = append(releasedPartitions, partitionId)
            }
        }
        if len(releasedPartitions) > 0 {
            dm.releaseDatasetPartitions(datasetName, releasedPartitions)
        }
    }
}

func (dm *datasetsManager) nodeDeleted(node Node) {
    // :TODO: Check only datasets previously owned by the deleted node
    datasetsWithPartitions, err := dm.ListDatasetsWithPartitions()
    if err != nil {
        panic(err)
    }

    for dataset, partitions := range datasetsWithPartitions {
        ownedPartitions := make([]string, 0)
        for _, partitionId := range partitions {
            shouldOwn, err := dm.shouldOwn(dataset.Meta().GetName(), partitionId)
            if err != nil {
                panic(err)
            }
            if shouldOwn {
                ownedPartitions = append(ownedPartitions, partitionId)
            }
        }
        if len(ownedPartitions) > 0 {
            dm.buildDatasetPartitions(dataset, ownedPartitions)
        }
    }
}

func (dm *datasetsManager) buildDatasetPartitions(dataset Dataset, partitions []string) {
    log.Infof("Own dataset: %s, partitions: %s", dataset.Meta().GetName(), partitions)
    dm.addLocalDataset(dataset.Meta().GetName(), partitions)
}

func (dm *datasetsManager) releaseDatasetPartitions(datasetName string, partitions []string) {
    log.Infof("Release dataset: %s, partitions: %s", datasetName, partitions)
    dm.deleteLocalDataset(datasetName)
}

func (dm *datasetsManager) shouldOwn(datasetName, partitionId string) (bool, error) {
    node, err := dm.cluster.GetHrwNode(fmt.Sprintf("%s.%s", datasetName, partitionId))
    if err != nil {
        return false, err
    }

    return node.Meta().GetUuid() == dm.cluster.Uuid(), nil
}
 
func (dm *datasetsManager) ListDatasetsWithPartitions() (map[Dataset][]string, error) {
    datasets, _, err := dm.zk.Children(dm.zkDatasetsPath())
    if err != nil {
        return nil, err
    }

    result := make(map[Dataset][]string)
    for _, datasetName := range datasets {
        dataset, err := dm.GetDataset(datasetName)
        if err != nil {
            return nil, err
        }
        partitions, err := dm.listDatasetPartitions(datasetName)
        if err != nil {
            return nil, err
        }
        result[dataset] = partitions
    }
    return result, nil
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

    return newDatasetFromProto(datasetMeta, dm.storage), nil
}

func (dm *datasetsManager) listDatasetPartitions(name string) ([]string, error) {
    partitions, _, err := dm.zk.Children(filepath.Join(dm.zkDatasetsPath(), name, "partitions"))

    return partitions, err
}

func (dm *datasetsManager) zkDatasetsPath() string {
    return filepath.Join(dm.config.Zookeeper.BasePath, "datasets")
}
