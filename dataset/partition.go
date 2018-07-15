package dataset

import (
    "fmt";
    "context";
    "crypto/sha256";
    "path/filepath";
    "time";

    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    log "github.com/Sirupsen/logrus";
)

type Partition interface {
    Meta() *pb.DatasetPartition
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
    storage storage.Storage
    index index.Index
}

func newPartitionFromProto(datasetMeta *pb.Dataset, meta *pb.DatasetPartition, ctx context.Context, config DatasetManagerConfig, zk utils.Zookeeper, storage storage.Storage) Partition {
    ctx, cancel := context.WithCancel(ctx)

    return &partition {
        datasetMeta: datasetMeta,
        meta: meta,
        ctx: ctx,
        cancel: cancel,
        config: config,
        zk: zk,
        storage: storage,
        index: index.FromProto(datasetMeta.GetIndex()),
    }
}

func (p *partition) Meta() *pb.DatasetPartition {
    return p.meta
}

func (p *partition) Load() error {
    log.Infof("Load: dataset: %s, partition: %d", p.datasetMeta.GetName(), p.meta.GetId())
    LOAD_INDEX:
    indexExists, err := p.storage.Exists(p.indexPath())
    if err != nil {
        return err
    }

    if indexExists {
        return p.loadIndex() 
    }

    buildLockExists, err := p.zk.Exists(p.zkBuildLockPath())
    if err != nil {
        return err
    }

    if buildLockExists {
        log.Infof("Wait for index - dataset: %s, partition: %d", p.datasetMeta.GetName(), p.meta.GetId())
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

    return p.buildIndex()
}

func (p *partition) Unload() error {
    log.Infof("Unload: dataset: %s, partition: %d", p.datasetMeta.GetName(), p.meta.GetId())
    p.cancel()
    p.index = nil
    return nil
}

func (p *partition) loadIndex() error {
    log.Infof("Load index - dataset: %s, partition: %d", p.datasetMeta.GetName(), p.meta.GetId())
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
    log.Infof("Build index - dataset: %s, partition: %d", p.datasetMeta.GetName(), p.meta.GetId())
    if _, err := p.zk.CreatePath(p.zkBuildLockPath(), nil, zk.FlagEphemeral); err != nil {
        return err
    }
    defer p.zk.Delete(p.zkBuildLockPath())

    select {
    case <- time.After(10 * time.Second):  // Populate/build
        log.Infof("Index build done - dataset: %s, partition: %d", p.datasetMeta.GetName(), p.meta.GetId())
    case <- p.ctx.Done():
        log.Infof("Cancel index build - dataset: %s, partition: %d", p.datasetMeta.GetName(), p.meta.GetId())
        return nil
    }

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

func (p *partition) indexPath() string {
    return filepath.Join(p.config.IndicesPath, p.indexFilename())
}

func (p *partition) indexFilename() string {
    hash := sha256.Sum256([]byte(fmt.Sprintf("%s.%d", p.datasetMeta.GetName(), p.Meta().GetId())))

    return fmt.Sprintf("%x.ti", hash)
}

func (p *partition) zkBuildLockPath() string {
    return filepath.Join(ZkDatasetsPath, p.datasetMeta.GetName(), "partition_build_locks", fmt.Sprintf("%d", p.Meta().GetId()))
}
