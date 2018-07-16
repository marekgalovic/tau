package dataset

import (
    "fmt";
    "io";
    "context";
    "crypto/sha256";
    "path/filepath";
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
    cancel context.CancelFunc

    config DatasetManagerConfig
    zk utils.Zookeeper
    cluster cluster.Cluster
    storage storage.Storage
    index index.Index

    log *log.Entry
}

func newPartitionFromProto(datasetMeta *pb.Dataset, meta *pb.DatasetPartition, ctx context.Context, config DatasetManagerConfig, zk utils.Zookeeper, cluster cluster.Cluster, storage storage.Storage) Partition {
    ctx, cancel := context.WithCancel(ctx)

    return &partition {
        datasetMeta: datasetMeta,
        meta: meta,
        ctx: ctx,
        cancel: cancel,
        config: config,
        zk: zk,
        cluster: cluster,
        storage: storage,
        index: index.FromProto(datasetMeta.GetIndex()),
        log: log.WithFields(log.Fields{
            "dataset_name": datasetMeta.GetName(),
            "partition_id": meta.GetId(),
        }),
    }
}

func (p *partition) Meta() *pb.DatasetPartition {
    return p.meta
}

func (p *partition) Index() index.Index {
    return p.index
}

func (p *partition) GetNode() (cluster.Node, error) {
    return p.cluster.GetHrwNode(fmt.Sprintf("%s.%d", p.datasetMeta.GetName(), p.Meta().GetId()))
}

func (p *partition) Load() error {
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
            case e := <-event:
                if e.Type == zk.EventNodeDeleted {
                    goto LOAD_INDEX
                }
                continue
            case <-p.ctx.Done():
                return nil
            }
        }
    }

    if err := p.buildIndex(); err != nil {
        return err
    }
    return p.registerNode()
}

func (p *partition) Unload() error {
    p.log.Info("Unload partition")
    if err := p.unregisterNode(); err != nil {
        return err
    }
    p.cancel()
    p.index.Reset()
    return nil
}

func (p *partition) loadIndex() error {
    p.log.WithFields(log.Fields{
        "index_path": p.indexPath(),
    }).Info("Load index")
    indexFile, err := p.storage.Reader(p.indexPath())
    if err != nil {
        return err
    }
    defer indexFile.Close()

    if err := p.index.Load(indexFile); err != nil {
        return err
    }

    return nil
}

func (p *partition) buildIndex() error {
    if _, err := p.zk.CreatePath(p.zkBuildLockPath(), nil, zk.FlagEphemeral); err != nil {
        return err
    }
    defer p.zk.Delete(p.zkBuildLockPath())

    p.log.Info("Build index")
    start := time.Now()
    p.index.Build(p.ctx)
    p.log.Info("Index building done: %s", time.Since(start))

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
    p.log.Info("Populate index")

    for _, filePath := range p.Meta().GetFiles() {
        file, err := p.storage.Reader(filePath)
        if err != nil {
            return err
        }
        defer file.Close()

        csv := serde.NewCsvReader(file)
        for {
            id, item, err := csv.ReadItem()
            if err == io.EOF {
                break
            }
            if err != nil {
                return err
            }
            p.index.Add(id, item)

            select {
            case <- p.ctx.Done():
                return nil
            default:
            }
        }
    }
    p.log.Infof("Index populated. Items: %d", p.index.Len())

    return nil
}

func (p *partition) registerNode() error {
    _, err := p.zk.Create(p.zkNodePath(), nil, zk.FlagEphemeral)

    return err
}

func (p *partition) unregisterNode() error {
    return p.zk.Delete(p.zkNodePath())
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

func (p *partition) zkNodePath() string {
    return filepath.Join(p.zkNodesPath(), p.cluster.Uuid())
}

func (p *partition) zkBuildLockPath() string {
    return filepath.Join(ZkDatasetsPath, p.datasetMeta.GetName(), "partition_build_locks", fmt.Sprintf("%d", p.Meta().GetId()))
}
