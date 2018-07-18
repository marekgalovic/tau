package dataset

import (
    "fmt";
    "io";
    "context";
    "crypto/sha256";
    "path/filepath";
    "sync";
    "time";

    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/cluster";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/storage/serde";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    log "github.com/Sirupsen/logrus";
)

type Partition interface {
    Meta() *pb.DatasetPartition
    Index() index.Index
    GetNode() (cluster.Node, error)
    Load() error
    Unload() error
}

type partition struct {
    datasetMeta *pb.Dataset
    meta *pb.DatasetPartition

    ctx context.Context
    loadCtx context.Context
    loadCancel context.CancelFunc

    config DatasetManagerConfig
    zk utils.Zookeeper
    cluster cluster.Cluster
    storage storage.Storage

    index index.Index
    indexMutex *sync.Mutex
    nodes utils.Set

    log *log.Entry
}

func newPartitionFromProto(datasetMeta *pb.Dataset, meta *pb.DatasetPartition, ctx context.Context, config DatasetManagerConfig, zk utils.Zookeeper, cluster cluster.Cluster, storage storage.Storage) Partition {
    p := &partition {
        datasetMeta: datasetMeta,
        meta: meta,
        ctx: ctx,
        config: config,
        zk: zk,
        cluster: cluster,
        storage: storage,
        index: index.FromProto(datasetMeta.GetIndex()),
        indexMutex: &sync.Mutex{},
        nodes: utils.NewThreadSafeSet(),
        log: log.WithFields(log.Fields{
            "dataset_name": datasetMeta.GetName(),
            "partition_id": meta.GetId(),
        }),
    }

    go p.watchNodes()

    return p
}

func (p *partition) Meta() *pb.DatasetPartition {
    return p.meta
}

func (p *partition) Index() index.Index {
    return p.index
}

func (p *partition) GetNode() (cluster.Node, error) {
    uuid := p.nodes.Rand()
    if uuid == nil {
        return nil, fmt.Errorf("No nodes")
    }

    return p.cluster.GetNode(uuid.(string))
}

func (p *partition) Load() error {
    p.loadCtx, p.loadCancel = context.WithCancel(p.ctx)

    if err := p.populateIndex(); err != nil {
        return err
    }

    LOAD_INDEX:
    indexExists, err := p.storage.Exists(p.indexPath())
    if err != nil {
        return err
    }

    if indexExists {
        if err := p.loadIndex(); err != nil {
            return err
        }
        return p.registerNode()
    }

    buildLockExists, err := p.zk.Exists(p.zkBuildLockPath())
    if err != nil {
        return err
    }

    if buildLockExists {
        p.log.Info("Wait for index")
        for {
            _, event, err := p.zk.ExistsW(p.zkBuildLockPath())
            if err != nil {
                return err
            }

            select {
            case <-p.loadCtx.Done():
                return nil
            case e := <-event:
                if e.Type == zk.EventNodeDeleted {
                    goto LOAD_INDEX
                }
                continue
            }
        }
    }

    if err := p.buildIndex(); err != nil {
        return err
    }
    return p.registerNode()
}

func (p *partition) Unload() error {
    defer p.indexMutex.Unlock()
    p.indexMutex.Lock()

    if p.loadCancel == nil {
        return fmt.Errorf("Unoad called before load")
    }
    if err := p.unregisterNode(); err != nil {
        return err
    }
    p.log.Info("Unload partition")
    p.loadCancel()
    p.index.Reset()
    return nil
}

func (p *partition) loadIndex() error {
    indexFile, err := p.storage.Reader(p.indexPath())
    if err != nil {
        return err
    }
    defer indexFile.Close()

    defer p.indexMutex.Unlock()
    p.indexMutex.Lock()
    if err := p.index.Load(indexFile); err != nil {
        return err
    }

    p.log.WithFields(log.Fields{
        "index_path": p.indexPath(),
    }).Info("Index loaded")

    return nil
}

func (p *partition) buildIndex() error {
    if _, err := p.zk.CreatePath(p.zkBuildLockPath(), nil, zk.FlagEphemeral); err != nil {
        return err
    }
    defer p.zk.Delete(p.zkBuildLockPath())

    start := time.Now()
    p.indexMutex.Lock()
    p.index.Build(p.ctx)
    p.indexMutex.Unlock()
    p.log.WithFields(log.Fields{
        "duration": time.Since(start),
    }).Info("Index built")

    indexFile, err := p.storage.Writer(p.indexPath())
    if err != nil {
        return err
    }
    defer indexFile.Close()
    
    if err := p.index.Save(indexFile); err != nil {
        return err
    }

    return nil
}

func (p *partition) populateIndex() error {
    defer p.indexMutex.Unlock()
    p.indexMutex.Lock()
    
    for _, filePath := range p.Meta().GetFiles() {
        file, err := p.storage.Reader(filePath)
        if err != nil {
            return err
        }
        defer file.Close()

        reader, err := serde.NewReaderFromProto(p.datasetMeta.GetFormat(), file)
        if err != nil {
            return err
        }
        for {
            id, item, err := reader.ReadItem()
            if err == io.EOF {
                break
            }
            if err != nil {
                return err
            }
            p.index.Add(id, item)

            select {
            case <- p.loadCtx.Done():
                return nil
            default:
            }
        }
    }
    p.log.WithFields(log.Fields{
        "items_count": p.index.Len(),
    }).Info("Index populated")

    return nil
}

func (p *partition) watchNodes() {
    changes, errors := p.zk.ChildrenChanges(p.ctx, p.zkNodesPath())

    for {
        select {
        case <-p.ctx.Done():
            return
        case event := <-changes:
            switch event.Type {
            case utils.EventZkWatchInit, utils.EventZkNodeCreated:
                p.nodes.Add(event.ZNode)

                p.log.WithFields(log.Fields{
                    "uuid": event.ZNode,
                }).Infof("New partition node")
            case utils.EventZkNodeDeleted:
                p.nodes.Remove(event.ZNode)

                p.log.WithFields(log.Fields{
                    "uuid": event.ZNode,
                }).Infof("Partition node deleted")
            }
        case err := <-errors:
            if err == zk.ErrNoNode {
                exists, err := p.zk.Exists(filepath.Join(ZkDatasetsPath, p.datasetMeta.GetName()))
                if err != nil {
                    panic(err)
                }
                if !exists {
                    return
                }
            }
            if (err == zk.ErrClosing) || (err == zk.ErrConnectionClosed) {
                return
            }
            if err != nil {
                panic(err)
            }
        }
    }
}

func (p *partition) registerNode() error {
    _, err := p.zk.Create(filepath.Join(p.zkNodesPath(), p.cluster.Uuid()), nil, zk.FlagEphemeral)

    return err
}

func (p *partition) unregisterNode() error {
    err := p.zk.Delete(filepath.Join(p.zkNodesPath(), p.cluster.Uuid()))
    if err == zk.ErrNoNode {
        return nil
    }
    
    return err
}

func (p *partition) indexPath() string {
    return filepath.Join(p.config.IndicesPath, p.indexFilename())
}

func (p *partition) indexFilename() string {
    hash := sha256.Sum256([]byte(fmt.Sprintf("%s.%d", p.datasetMeta.GetName(), p.Meta().GetId())))

    return fmt.Sprintf("%x.ti", hash)
}

func (p *partition) zkNodesPath() string {
    return filepath.Join(ZkDatasetsPath, p.datasetMeta.GetName(), "partitions", fmt.Sprintf("%d", p.Meta().GetId()))
}

func (p *partition) zkBuildLockPath() string {
    return filepath.Join(ZkDatasetsPath, p.datasetMeta.GetName(), "partition_build_locks", fmt.Sprintf("%d", p.Meta().GetId()))
}
