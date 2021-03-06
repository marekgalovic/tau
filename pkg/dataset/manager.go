package dataset

import (
    "fmt";
    "context";
    "sync";
    "path/filepath";


    "github.com/marekgalovic/tau/pkg/cluster";
    "github.com/marekgalovic/tau/pkg/storage";
    pb "github.com/marekgalovic/tau/pkg/protobuf";
    "github.com/marekgalovic/tau/pkg/utils";

    "github.com/samuel/go-zookeeper/zk";
    "github.com/golang/protobuf/proto";
    log "github.com/Sirupsen/logrus";
)

const ZkDatasetsPath string = "datasets"

const (
    EventDatasetCreated int32 = 1
    EventDatasetDeleted int32 = 2
)

type DatasetsChangedNotification struct {
    Event int32
    Dataset Dataset
}

type DatasetManagerConfig struct {
    IndicesPath string
}

type Manager interface {
    GetDataset(string) (Dataset, bool)
    Run()
}

type manager struct {
    ctx context.Context
    cancel context.CancelFunc
    config DatasetManagerConfig
    zk utils.Zookeeper
    cluster cluster.Cluster
    storage storage.Storage

    datasets map[string]Dataset
    datasetsMutex *sync.Mutex
    datasetChangesNotifications utils.Broadcast

    localDatasets utils.Set
}

func NewManager(config DatasetManagerConfig, zk utils.Zookeeper, cluster cluster.Cluster, storage storage.Storage) (*manager, error) {
    ctx, cancel := context.WithCancel(context.Background())

    m := &manager {
        ctx: ctx,
        cancel: cancel,
        config: config,
        zk: zk,
        cluster: cluster,
        storage: storage,
        datasets: make(map[string]Dataset),
        datasetsMutex: &sync.Mutex{},
        datasetChangesNotifications: utils.NewThreadSafeBroadcast(),
        localDatasets: utils.NewThreadSafeSet(),
    }
    if err := m.bootstrapZk(); err != nil {
        return nil, err
    }

    return m, nil
}

func (m *manager) bootstrapZk() error {
    paths := []string{ZkDatasetsPath}

    for _, path := range paths {
        _, err := m.zk.CreatePath(path, nil, int32(0))
        if err == zk.ErrNodeExists {
            continue
        }
        if err != nil {
            return err
        }  
    }
    return nil
}

func (m *manager) Run() {
    go m.watchDatasets()
    go m.run()
}

func (m *manager) GetDataset(name string) (Dataset, bool) {
    defer m.datasetsMutex.Unlock()
    m.datasetsMutex.Lock()

    dataset, exists := m.datasets[name]
    return dataset, exists
}

func (m *manager) addDataset(dataset Dataset) {
    defer m.datasetsMutex.Unlock()
    m.datasetsMutex.Lock()

    m.datasets[dataset.Meta().GetName()] = dataset
}

func (m *manager) deleteDataset(name string) {
    defer m.datasetsMutex.Unlock()
    m.datasetsMutex.Lock()

    delete(m.datasets, name)
}

func (m *manager) watchDatasets() {
    changes, errors := m.zk.ChildrenChanges(m.ctx, ZkDatasetsPath)

    for {
        select {
        case <-m.ctx.Done():
            return
        case event := <-changes:
            switch event.Type {
            case utils.EventZkWatchInit, utils.EventZkNodeCreated:
                datasetData, err := m.getDatasetData(event.ZNode)
                if err != nil {
                    panic(err)
                }
                dataset, err := newDatasetFromProto(datasetData, m.ctx, m.config, m.zk, m.cluster, m.storage)
                if err != nil {
                    panic(err)
                }
                
                log.WithFields(log.Fields{
                    "dataset_name": dataset.Meta().GetName(),
                }).Info("New created")

                m.addDataset(dataset)
                m.datasetChangesNotifications.Send(&DatasetsChangedNotification {
                    Event: EventDatasetCreated,
                    Dataset: dataset,
                })
            case utils.EventZkNodeDeleted:
                dataset, _ := m.GetDataset(event.ZNode)
                log.WithFields(log.Fields{
                    "dataset_name": dataset.Meta().GetName(),
                }).Info("Dataset deleted")

                m.deleteDataset(event.ZNode)
                m.datasetChangesNotifications.Send(&DatasetsChangedNotification {
                    Event: EventDatasetDeleted,
                    Dataset: dataset,
                })
            }
        case err := <-errors:
            if (err == zk.ErrClosing) || (err == zk.ErrConnectionClosed) {
                return
            }
            if err != nil {
                panic(err)
            }
        }
    }
}

func (m *manager) run() {
    datasetNotifications := m.datasetChangesNotifications.Listen(10)
    clusterNotifications := m.cluster.NodeChanges()

    for {
        select {
        case <-m.ctx.Done():
            return
        case n := <-clusterNotifications:
            notification := n.(*cluster.NodesChangedNotification)

            switch notification.Event {
            case cluster.EventNodeCreated:
                go m.nodeCreated(notification.Node)
            case cluster.EventNodeDeleted:
                go m.nodeDeleted(notification.Node)
            }

        case n := <-datasetNotifications:
            notification := n.(*DatasetsChangedNotification)

            switch notification.Event {
            case EventDatasetCreated:
                go m.datasetCreated(notification.Dataset)
            case EventDatasetDeleted:
                go m.datasetDeleted(notification.Dataset)
            }
        }
    }
}

func (m *manager) nodeCreated(node cluster.Node) {
    for _, datasetName := range m.localDatasets.ToSlice() {
        dataset, exists := m.GetDataset(datasetName.(string))
        if !exists {
            panic("Dataset does not exists")
        }

        releasedPartitions := utils.NewSet()
        for _, partitionId := range dataset.LocalPartitions() {
            shouldOwn, err := m.shouldOwn(dataset, partitionId.(string))
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

func (m *manager) nodeDeleted(node cluster.Node) {
    // :TODO: Check only datasets previously owned by the deleted node
    datasetsWithPartitions, err := m.listDatasetsWithPartitions()
    if err != nil {
        panic(err)
    }

    for datasetName, partitions := range datasetsWithPartitions {
        dataset, exists := m.GetDataset(datasetName)
        if !exists {
            panic("Dataset does not exists")
        }

        ownedPartitions := utils.NewSet()
        for _, partitionId := range partitions {
            shouldOwn, err := m.shouldOwn(dataset, partitionId)
            if err != nil {
                panic(err)
            }
            if shouldOwn {
                ownedPartitions.Add(partitionId)
            }  
        }

        if ownedPartitions.Len() > 0 {
            m.localDatasets.Add(dataset.Meta().GetName())
            dataset.BuildPartitions(ownedPartitions)
        }
    }
}

func (m *manager) datasetCreated(dataset Dataset) {
    partitions, err := m.listDatasetPartitions(dataset.Meta().GetName())
    if err != nil {
        panic(err)
    }

    ownedPartitions := utils.NewSet()
    for _, partitionId := range partitions {
        shouldOwn, err := m.shouldOwn(dataset, partitionId)
        if err != nil {
            panic(err)
        }
        if shouldOwn {
            ownedPartitions.Add(partitionId)
        }
    }
    
    if ownedPartitions.Len() > 0 {
        m.localDatasets.Add(dataset.Meta().GetName())
        if err := dataset.BuildPartitions(ownedPartitions); err != nil {
            log.Errorf("Failed to load dataset. Err: %v", err)
        }
    }
}

func (m *manager) datasetDeleted(dataset Dataset) {
    if m.localDatasets.Contains(dataset.Meta().GetName()) {
        m.localDatasets.Remove(dataset.Meta().GetName())
        if err := dataset.DeleteAllPartitions(); err != nil {
            log.Errorf("Failed to delete dataset. Err: %v", err)
            return
        }

        log.WithFields(log.Fields{
            "dataset_name": dataset.Meta().GetName(),
        }).Info("Delete local")
    }
}

func (m *manager) shouldOwn(dataset Dataset, partitionId string) (bool, error) {
    nodes, err := m.cluster.GetTopHrwNodes(int(dataset.Meta().GetNumReplicas()), fmt.Sprintf("%s.%s", dataset.Meta().GetName(), partitionId))
    if err != nil {
        return false, err
    }

    return nodes.Contains(m.cluster.Uuid()), nil
}

func (m *manager) getDatasetData(name string) (*pb.Dataset, error) {
    zkPath := filepath.Join(ZkDatasetsPath, name)
    datasetData, err := m.zk.Get(zkPath)
    if err != nil {
        return nil, err
    }

    datasetMeta := &pb.Dataset{}
    if err = proto.Unmarshal(datasetData, datasetMeta); err != nil {
        return nil, err
    }

    return datasetMeta, nil
}

func (m *manager) listDatasetPartitions(name string) ([]string, error) {
    partitions, err := m.zk.Children(filepath.Join(ZkDatasetsPath, name, "partitions"))
    if err != nil {
        return nil, err
    }

    return partitions, nil
}

func (m *manager) listDatasetsWithPartitions() (map[string][]string, error) {
    datasets, err := m.zk.Children(ZkDatasetsPath)
    if err != nil {
        return nil, err
    }

    datasetsWithPartitions := make(map[string][]string)
    for _, dataset := range datasets {
        partitions, err := m.listDatasetPartitions(dataset)
        if err != nil {
            return nil, err
        }
        datasetsWithPartitions[dataset] = partitions
    }
    return datasetsWithPartitions, nil
}
