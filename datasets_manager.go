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

    localDatasets utils.Set
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

        localDatasets: utils.NewThreadSafeSet(),
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
    datasetsWithPartitions, err := dm.listDatasetsWithPartitions()
    if err != nil {
        return err
    }

    wg := &sync.WaitGroup{}
    for datasetName, partitions := range datasetsWithPartitions {
        datasetData, err := dm.getDatasetData(datasetName)
        if err != nil {
            return err
        }
        dataset := newDatasetFromProto(datasetData, dm.ctx, dm.config, dm.zk, dm.storage)
        dm.addDataset(dataset)

        ownedPartitions := utils.NewSet()
        for _, partitionId := range partitions {
            shouldOwn, err := dm.shouldOwn(dataset, partitionId)
            if err != nil {
                return err
            }
            if shouldOwn {
                ownedPartitions.Add(partitionId)
            }
        }

        if ownedPartitions.Len() > 0 {
            dm.localDatasets.Add(dataset.Meta().GetName())

            wg.Add(1)
            go func(dataset Dataset, partitions utils.Set) {
                defer wg.Done()
                dataset.BuildPartitions(partitions)
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
            datasetData, err := dm.getDatasetData(datasetName)
            if err != nil {
                return err
            }
            dataset := newDatasetFromProto(datasetData, dm.ctx, dm.config, dm.zk, dm.storage)
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
    for _, partitionId := range partitions {
        shouldOwn, err := dm.shouldOwn(dataset, partitionId)
        if err != nil {
            panic(err)
        }
        if shouldOwn {
            ownedPartitions.Add(partitionId)
        }
    }
    
    if ownedPartitions.Len() > 0 {
        dm.localDatasets.Add(dataset.Meta().GetName())
        dataset.BuildPartitions(ownedPartitions)
    }
}

func (dm *datasetsManager) datasetDeleted(dataset Dataset) {
    if dm.localDatasets.Contains(dataset.Meta().GetName()) {
        log.Infof("Delete local: %s", dataset.Meta().GetName())
        dm.localDatasets.Remove(dataset.Meta().GetName())
    }
}

func (dm *datasetsManager) nodeCreated(node Node) {
    for _, datasetName := range dm.localDatasets.ToSlice() {
        dataset, exists := dm.getDataset(datasetName.(string))
        if !exists {
            panic("Dataset does not exists")
        }

        releasedPartitions := utils.NewSet()
        for _, partitionId := range dataset.LocalPartitions() {
            shouldOwn, err := dm.shouldOwn(dataset, partitionId.(string))
            if err != nil {
                panic(err)
            }
            if !shouldOwn {
                releasedPartitions.Add(partitionId)
            }
        }

        if releasedPartitions.Len() > 0 {
            dataset.DeletePartitions(releasedPartitions)
        }
    }
}

func (dm *datasetsManager) nodeDeleted(node Node) {
    // :TODO: Check only datasets previously owned by the deleted node
    datasetsWithPartitions, err := dm.listDatasetsWithPartitions()
    if err != nil {
        panic(err)
    }

    for datasetName, partitions := range datasetsWithPartitions {
        dataset, exists := dm.getDataset(datasetName)
        if !exists {
            panic("Dataset does not exists")
        }

        ownedPartitions := utils.NewSet()
        for _, partitionId := range partitions {
            shouldOwn, err := dm.shouldOwn(dataset, partitionId)
            if err != nil {
                panic(err)
            }
            if shouldOwn {
                ownedPartitions.Add(partitionId)
            }  
        }

        if ownedPartitions.Len() > 0 {
            dm.localDatasets.Add(dataset.Meta().GetName())
            dataset.BuildPartitions(ownedPartitions)
        }
    }
}

func (dm *datasetsManager) shouldOwn(dataset Dataset, partitionId string) (bool, error) {
    nodes, err := dm.cluster.GetTopHrwNodes(int(dataset.Meta().GetNumReplicas()), fmt.Sprintf("%s.%s", dataset.Meta().GetName(), partitionId))
    if err != nil {
        return false, err
    }

    return nodes.Contains(dm.cluster.Uuid()), nil
}

func (dm *datasetsManager) getDatasetData(name string) (*pb.Dataset, error) {
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

    return datasetMeta, nil
}
 
func (dm *datasetsManager) listDatasetsWithPartitions() (map[string][]string, error) {
    datasets, _, err := dm.zk.Children(dm.zkDatasetsPath())
    if err != nil {
        return nil, err
    }

    result := make(map[string][]string)
    for _, datasetName := range datasets {
        partitions, err := dm.listDatasetPartitions(datasetName)
        if err != nil {
            return nil, err
        }
        result[datasetName] = partitions
    }
    return result, nil
}

func (dm *datasetsManager) listDatasetPartitions(name string) ([]string, error) {
    partitions, _, err := dm.zk.Children(filepath.Join(dm.zkDatasetsPath(), name, "partitions"))
    if err != nil {
        return nil, err
    }

    return partitions, nil
}

func (dm *datasetsManager) zkDatasetsPath() string {
    return filepath.Join(dm.config.Zookeeper.BasePath, "datasets")
}
