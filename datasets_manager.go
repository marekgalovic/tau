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

    localDatasets map[string]utils.Set
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

        localDatasets: make(map[string]utils.Set),
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

func (dm *datasetsManager) getDataset(name string) (Dataset, bool) {
    defer dm.datasetsMutex.Unlock()
    dm.datasetsMutex.Lock()

    dataset, exists := dm.datasets[name]
    return dataset, exists
}

func (dm *datasetsManager) deleteDataset(name string) {
    defer dm.datasetsMutex.RUnlock()
    dm.datasetsMutex.RLock()

    delete(dm.datasets, name)
}

func (dm *datasetsManager) addLocalDataset(name string, partitions utils.Set) {
    defer dm.localDatasetsMutex.Unlock()
    dm.localDatasetsMutex.Lock()

    dm.localDatasets[name] = partitions
}

func (dm *datasetsManager) getLocalDataset(name string) (utils.Set, bool) {
    defer dm.localDatasetsMutex.Unlock()
    dm.localDatasetsMutex.Lock()

    partitions, exists := dm.localDatasets[name]
    return partitions, exists
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

    wg := &sync.WaitGroup{}
    for dataset, partitions := range datasetsWithPartitions {
        dm.addDataset(dataset)

        ownedPartitions := utils.NewSet()
        for partitionId := range partitions.ToIterator() {
            shouldOwn, err := dm.shouldOwn(dataset, partitionId.(string))
            if err != nil {
                return err
            }
            if shouldOwn {
                ownedPartitions.Add(partitionId)
            }
        }
        if ownedPartitions.Len() > 0 {
            wg.Add(1)
            go func(dataset Dataset, partitions utils.Set) {
                defer wg.Done()
                dm.buildDatasetPartitions(dataset, partitions)
            }(dataset, ownedPartitions)
        }
    }
    wg.Wait()

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

        if _, exists := dm.getDataset(datasetName); !exists {
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

    defer dm.datasetsMutex.Unlock()
    dm.datasetsMutex.Lock()
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

    ownedPartitions := utils.NewSet()
    for partitionId := range partitions.ToIterator() {
        shouldOwn, err := dm.shouldOwn(dataset, partitionId.(string))
        if err != nil {
            panic(err)
        }
        if shouldOwn {
            ownedPartitions.Add(partitionId)
        }
    }
    if ownedPartitions.Len() > 0 {
        dm.buildDatasetPartitions(dataset, ownedPartitions)
    }
}

func (dm *datasetsManager) datasetDeleted(dataset Dataset) {
    if _, exists := dm.getLocalDataset(dataset.Meta().GetName()); exists {
        log.Infof("Delete local: %s", dataset.Meta().GetName())
        dm.deleteLocalDataset(dataset.Meta().GetName())
    }
}

func (dm *datasetsManager) nodeCreated(node Node) {
    for datasetName, partitions := range dm.localDatasets {
        releasedPartitions := utils.NewSet()
        for partitionId := range partitions.ToIterator() {
            dataset, _ := dm.getDataset(datasetName)
            shouldOwn, err := dm.shouldOwn(dataset, partitionId.(string))
            if err != nil {
                panic(err)
            }
            if !shouldOwn {
                releasedPartitions.Add(partitionId)
            }
        }
        if releasedPartitions.Len() > 0 {
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
        ownedPartitions := utils.NewSet()
        for partitionId := range partitions.ToIterator() {
            shouldOwn, err := dm.shouldOwn(dataset, partitionId.(string))
            if err != nil {
                panic(err)
            }
            if shouldOwn {
                ownedPartitions.Add(partitionId)
            }
        }
        if ownedPartitions.Len() > 0 {
            dm.buildDatasetPartitions(dataset, ownedPartitions)
        }
    }
}

func (dm *datasetsManager) buildDatasetPartitions(dataset Dataset, partitions utils.Set) {
    existingPartitions, exists := dm.getLocalDataset(dataset.Meta().GetName())
    if !exists {
        dm.addLocalDataset(dataset.Meta().GetName(), partitions)
        log.Infof("Own dataset: %s, partitions: %s", dataset.Meta().GetName(), partitions)
    } else {
        dm.addLocalDataset(dataset.Meta().GetName(), existingPartitions.Union(partitions))
        log.Infof("Own dataset: %s, partitions: %s, new partitions: %s", dataset.Meta().GetName(), existingPartitions.Union(partitions), partitions.Difference(existingPartitions))
    }
}

func (dm *datasetsManager) releaseDatasetPartitions(datasetName string, partitions utils.Set) {
    existingPartitions, _ := dm.getLocalDataset(datasetName)
    dm.addLocalDataset(datasetName, existingPartitions.Difference(partitions))

    log.Infof("Release dataset: %s, partitions: %s, released partitions: %s", datasetName, existingPartitions.Difference(partitions), partitions)
}

func (dm *datasetsManager) shouldOwn(dataset Dataset, partitionId string) (bool, error) {
    nodes, err := dm.cluster.GetTopHrwNodes(int(dataset.Meta().GetNumReplicas()), fmt.Sprintf("%s.%s", dataset.Meta().GetName(), partitionId))
    if err != nil {
        return false, err
    }

    return nodes.Contains(dm.cluster.Uuid()), nil
}
 
func (dm *datasetsManager) ListDatasetsWithPartitions() (map[Dataset]utils.Set, error) {
    datasets, _, err := dm.zk.Children(dm.zkDatasetsPath())
    if err != nil {
        return nil, err
    }

    result := make(map[Dataset]utils.Set)
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

func (dm *datasetsManager) listDatasetPartitions(name string) (utils.Set, error) {
    partitions, _, err := dm.zk.Children(filepath.Join(dm.zkDatasetsPath(), name, "partitions"))
    if err != nil {
        return nil, err
    }

    result := utils.NewSet()
    for _, partition := range partitions {
        result.Add(partition)
    }
    return result, nil
}

func (dm *datasetsManager) zkDatasetsPath() string {
    return filepath.Join(dm.config.Zookeeper.BasePath, "datasets")
}
