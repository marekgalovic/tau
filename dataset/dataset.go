package dataset

import (
    "context";
    "sync";

    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    log "github.com/Sirupsen/logrus";
)

type Dataset interface {
    Meta() *pb.Dataset
    LocalPartitions() []interface{}
    BuildPartitions(utils.Set) error
    DeletePartitions(utils.Set) error
    DeleteAllPartitions() error
}

type dataset struct {
    ctx context.Context
    meta *pb.Dataset
    zk utils.Zookeeper
    storage storage.Storage

    partitions utils.Set
    partitionIndices map[string]index.Index
    partitionIndicesMutex *sync.Mutex
}

func newDatasetFromProto(meta *pb.Dataset, ctx context.Context, zk utils.Zookeeper, storage storage.Storage) Dataset {
    return &dataset {
        ctx: ctx,
        meta: meta,
        zk: zk,
        storage: storage,
        partitions: utils.NewThreadSafeSet(),
    }
}

func (d *dataset) Meta() *pb.Dataset {
    return d.meta
}

func (d *dataset) LocalPartitions() []interface{} {
    return d.partitions.ToSlice()
} 

func (d *dataset) BuildPartitions(updatedPartitions utils.Set) error {
    newPartitions := updatedPartitions.Difference(d.partitions)
    if newPartitions.Len() == 0 {
        return nil    
    }
    d.partitions = d.partitions.Union(updatedPartitions)

    for _, partitionId := range newPartitions.ToSlice() {
        go d.loadPartition(partitionId.(string))   
    }

    return nil
}

func (d *dataset) DeletePartitions(deletedPartitions utils.Set) error {
    d.partitions = d.partitions.Difference(deletedPartitions)

    for _, partitionId := range deletedPartitions.ToSlice() {
        go d.unloadPartition(partitionId.(string))
    }

    return nil
}

func (d *dataset) DeleteAllPartitions() error {
    for _, partitionId := range d.partitions.ToSlice() {
        go d.unloadPartition(partitionId.(string))
    }

    return nil
}

func (d *dataset) loadPartition(partitionId string) {
    log.Infof("Dataset: `%s`, load partition: %s", d.Meta().GetName(), partitionId)
}

func (d *dataset) unloadPartition(partitionId string) {
    log.Infof("Dataset: `%s`, unload partition: %s", d.Meta().GetName(), partitionId)
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
