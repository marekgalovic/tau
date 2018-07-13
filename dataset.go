package tau

import (
    "fmt";
    "context";
    "sync";
    "time";
    "path/filepath";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/storage/serde";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/samuel/go-zookeeper/zk";
    log "github.com/Sirupsen/logrus";
)

type Dataset interface {
    Meta() *pb.Dataset
    LocalPartitions() []interface{}
    BuildPartitions(utils.Set) error
    DeletePartitions(utils.Set) error
    Search(int, math.Vector) ([]*pb.SearchResultItem, error)
    Load([]string) error
}

type dataset struct {
    ctx context.Context
    config *Config
    meta *pb.Dataset
    index *pb.Index

    partitions utils.Set
    partitionIndices map[string]index.Index
    partitionIndicesMutex *sync.Mutex

    zk *zk.Conn
    storage storage.Storage
}

func newDatasetFromProto(proto *pb.Dataset, ctx context.Context, config *Config, zkConn *zk.Conn, storage storage.Storage) Dataset {
    return &dataset {
        ctx: ctx,
        config: config,
        meta: proto,
        index: proto.Index,
        partitions: utils.NewThreadSafeSet(),
        partitionIndices: make(map[string]index.Index),
        partitionIndicesMutex: &sync.Mutex{},
        zk: zkConn,
        storage: storage,
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

func (d *dataset) loadPartition(partitionId string) {
    log.Infof("Dataset `%s` load partition: %s", d.Meta().GetName(), partitionId)

    LOAD_INDEX:
    indexExists, err := d.storage.Exists(d.partitionIndexPath(partitionId))
    if err != nil {
        panic(err)
    }
    if indexExists {
        log.Info("Load index file")
        indexFile, err := d.storage.Reader(d.partitionIndexPath(partitionId))
        if err != nil {
            panic(err)
        }
        defer indexFile.Close()

        partitionIndex := index.FromProto(d.index)
        if err := partitionIndex.Load(indexFile); err != nil {
            panic(err)
        }

        d.partitionIndicesMutex.Lock()
        d.partitionIndices[partitionId] = partitionIndex
        d.partitionIndicesMutex.Unlock()
        return
    }

    buildLockExists, _, err := d.zk.Exists(d.zkPartitionBuildLockPath(partitionId))
    if err != nil {
        panic(err)
    }

    if buildLockExists {
        log.Info("Wait for node to build partition index")
        for {
            _, _, event, err := d.zk.ExistsW(d.zkPartitionBuildLockPath(partitionId))
            if err != nil {
                panic(err)
            }

            select {
            case e := <- event:
                if e.Type == zk.EventNodeDeleted {
                    log.Info("Build partition lock released")
                    goto LOAD_INDEX
                }
                continue
            case <- d.ctx.Done():
                return
            }
        }
    }

    if err := utils.ZkCreatePath(d.zk, d.zkPartitionBuildLockPath(partitionId), nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll)); err != nil {
        panic(err)
    }
    defer func() {
        if err := d.zk.Delete(d.zkPartitionBuildLockPath(partitionId), -1); err != nil {
            panic(err)
        }
    }()

    log.Infof("Dataset `%s` build index for partition: %s", d.Meta().GetName(), partitionId)
    partitionIndex := index.FromProto(d.index)
    <- time.After(10 * time.Second)  // Populate/build

    indexFile, err := d.storage.Writer(d.partitionIndexPath(partitionId))
    if err != nil {
        panic(err)
    }
    defer indexFile.Close()
    
    if err := partitionIndex.Save(indexFile); err != nil {
        panic(err)
    }

    d.partitionIndicesMutex.Lock()
    d.partitionIndices[partitionId] = partitionIndex
    d.partitionIndicesMutex.Unlock()
}

func (d *dataset) unloadPartition(partitionId string) {
    defer d.partitionIndicesMutex.Unlock()
    d.partitionIndicesMutex.Lock()

    delete(d.partitionIndices, partitionId)
    log.Infof("Dataset `%s` unload partition: %s", d.Meta().GetName(), partitionId)
}

func (d *dataset) Search(k int, query math.Vector) ([]*pb.SearchResultItem, error) {
    return nil, nil
}

func (d *dataset) Load(files []string) error {
    return nil
    // if err := d.populateIndex(files, serde.NewCsv(",")); err != nil {
    //     return err
    // }

    // d.index.Build()

    // indexFile, err := d.storage.Writer(d.indexFileName(files))
    // if err != nil {
    //     return err
    // }

    // if err := d.index.Save(indexFile); err != nil {
    //     return err
    // }

    // return indexFile.Close()
}

func (d *dataset) populateIndex(files []string, deserializer serde.Deserializer) error {
    return nil
    // for _, filePath := range files {
    //     fileReader, err := d.storage.Reader(filePath)
    //     if err != nil {
    //         return err
    //     }

    //     bufReader := bufio.NewReader(fileReader)
    //     for {
    //         lineBytes, _, err := bufReader.ReadLine()
    //         if err != nil {
    //             break
    //         }

    //         id, vector, err := deserializer.DeserializeItem(lineBytes)
    //         if err != nil {
    //             return err
    //         }
    //         d.index.Add(id, vector)
    //     }
    // }
    // return nil
}

func (d *dataset) partitionIndexPath(partitionId string) string {
    return fmt.Sprintf("%s.ti", filepath.Join(d.config.IndexPath, d.Meta().GetName(), partitionId))
}

func (d *dataset) zkPartitionBuildLockPath(partitionId string) string {
    return filepath.Join(d.Meta().GetZkPath(), "partition_build_locks", partitionId)
} 
