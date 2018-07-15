package dataset

import (
    "fmt";
    "context";
    "crypto/sha256";

    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    log "github.com/Sirupsen/logrus"
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
    zk utils.Zookeeper
    storage storage.Storage
    index index.Index
}

func newPartitionFromProto(datasetMeta *pb.Dataset, meta *pb.DatasetPartition, ctx context.Context, zk utils.Zookeeper, storage storage.Storage) Partition {
    return &partition {
        datasetMeta: datasetMeta,
        meta: meta,
        ctx: ctx,
        zk: zk,
        storage: storage,
        index: index.FromProto(datasetMeta.GetIndex()),
    }
}

func (p *partition) Meta() *pb.DatasetPartition {
    return p.meta
}

func (p *partition) Load() error {
    log.Infof("load index file: %s", p.indexFilename())
    return nil
}

func (p *partition) Unload() error {
    return nil
}

func (p *partition) indexFilename() string {
    hash := sha256.Sum256([]byte(fmt.Sprintf("%s.%d", p.datasetMeta.GetName(), p.Meta().GetId())))

    return fmt.Sprintf("%x.ti", hash)
}

// func (d *dataset) loadPartition(partitionId string) {
//     log.Infof("Dataset `%s` load partition: %s", d.Meta().GetName(), partitionId)

//     LOAD_INDEX:
//     indexExists, err := d.storage.Exists(d.partitionIndexPath(partitionId))
//     if err != nil {
//         panic(err)
//     }
//     if indexExists {
//         log.Info("Load index file")
//         indexFile, err := d.storage.Reader(d.partitionIndexPath(partitionId))
//         if err != nil {
//             panic(err)
//         }
//         defer indexFile.Close()

//         partitionIndex := index.FromProto(d.index)
//         if err := partitionIndex.Load(indexFile); err != nil {
//             panic(err)
//         }

//         d.partitionIndicesMutex.Lock()
//         d.partitionIndices[partitionId] = partitionIndex
//         d.partitionIndicesMutex.Unlock()
//         return
//     }

//     buildLockExists, _, err := d.zk.Exists(d.zkPartitionBuildLockPath(partitionId))
//     if err != nil {
//         panic(err)
//     }

//     if buildLockExists {
//         log.Info("Wait for node to build partition index")
//         for {
//             _, _, event, err := d.zk.ExistsW(d.zkPartitionBuildLockPath(partitionId))
//             if err != nil {
//                 panic(err)
//             }

//             select {
//             case e := <- event:
//                 if e.Type == zk.EventNodeDeleted {
//                     log.Info("Build partition lock released")
//                     goto LOAD_INDEX
//                 }
//                 continue
//             case <- d.ctx.Done():
//                 return
//             }
//         }
//     }

//     if err := utils.ZkCreatePath(d.zk, d.zkPartitionBuildLockPath(partitionId), nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll)); err != nil {
//         panic(err)
//     }
//     defer func() {
//         if err := d.zk.Delete(d.zkPartitionBuildLockPath(partitionId), -1); err != nil {
//             panic(err)
//         }
//     }()

//     log.Infof("Dataset `%s` build index for partition: %s", d.Meta().GetName(), partitionId)
//     partitionIndex := index.FromProto(d.index)
//     <- time.After(10 * time.Second)  // Populate/build

//     indexFile, err := d.storage.Writer(d.partitionIndexPath(partitionId))
//     if err != nil {
//         panic(err)
//     }
//     defer indexFile.Close()
    
//     if err := partitionIndex.Save(indexFile); err != nil {
//         panic(err)
//     }

//     d.partitionIndicesMutex.Lock()
//     d.partitionIndices[partitionId] = partitionIndex
//     d.partitionIndicesMutex.Unlock()
// }
