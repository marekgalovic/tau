package tau

import (
    // "errors";
    "context";
    "path/filepath";

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
    datasetChangesNotifications utils.Broadcast

    localDatasets map[string]struct{}
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

        localDatasets: make(map[string]struct{}),
    }
    if err := dm.bootstrapZk(); err != nil {
        return nil, err
    }

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

func (dm *datasetsManager) Run() error {
    nodes, err := dm.cluster.ListNodes()
    if err != nil {
        return err
    }
    datasetsWithPartitions, err := dm.ListDatasetsWithPartitions()
    if err != nil {
        return err
    }

    log.Info(nodes)
    log.Info(datasetsWithPartitions)

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
    datasetNotifications := dm.datasetChangesNotifications.Listen(10)

    for {
        select {
        case n := <- nodeNotifications:
            notification := n.(*NodesChangedNotification)

            switch notification.Event {
            case EventNodeCreated:
                log.Infof("DM node created: %s", notification.Node.Meta().GetUuid())
                go dm.nodeCreated(notification.Node)
            case EventNodeDeleted:
                log.Infof("DM node deleted: %s", notification.Node.Meta().GetUuid())
                go dm.nodeDeleted(notification.Node)
            }
        case n := <- datasetNotifications:
            notification := n.(*DatasetsChangedNotification)

            switch notification.Event {
            case EventDatasetCreated:
                log.Infof("DM dataset created: %s", notification.Dataset.Meta().GetName())
                go dm.datasetCreated(notification.Dataset)
            case EventDatasetDeleted:
                log.Infof("DM dataset deleted: %s", notification.Dataset.Meta().GetName())
                go dm.datasetDeleted(notification.Dataset)
            }
        case <- dm.ctx.Done():
            return
        }
    }
}

func (dm *datasetsManager) datasetCreated(dataset Dataset) {
    node, err := dm.cluster.GetHrwNode(dataset.Meta().GetName())
    if err != nil {
        panic(err)
    }

    if node.Meta().GetUuid() == dm.cluster.Uuid() {
        log.Infof("Own: %s", dataset.Meta().GetName())
        dm.localDatasets[dataset.Meta().GetName()] = struct{}{}
    }
}

func (dm *datasetsManager) datasetDeleted(dataset Dataset) {
    if _, exists := dm.localDatasets[dataset.Meta().GetName()]; exists {
        log.Infof("Delete local: %s", dataset.Meta().GetName())
    }
}

func (dm *datasetsManager) nodeCreated(node Node) {
    for datasetName, _ := range dm.localDatasets {
        node, err := dm.cluster.GetHrwNode(datasetName)
        if err != nil {
            panic(err)
        }

        if node.Meta().GetUuid() != dm.cluster.Uuid() {
            log.Infof("Release ownership: %s", datasetName)
            delete(dm.localDatasets, datasetName)
        }
    }
}

func (dm *datasetsManager) nodeDeleted(node Node) {
    for _, dataset := range dm.datasets {
        topNode, err := dm.cluster.GetHrwNode(dataset.Meta().GetName())
        if err != nil {
            panic(err)
        }

        if topNode.Meta().GetUuid() == dm.cluster.Uuid() {
            log.Infof("Own: %s", dataset.Meta().GetName())
            dm.localDatasets[dataset.Meta().GetName()] = struct{}{}
        }
    }
}
 
func (dm *datasetsManager) ListDatasetsWithPartitions() (map[string][]string, error) {
    datasets, _, err := dm.zk.Children(dm.zkDatasetsPath())
    if err != nil {
        return nil, err
    }

    result := make(map[string][]string)
    for _, dataset := range datasets {
        partitions, err := dm.listDatasetPartitions(dataset)
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
