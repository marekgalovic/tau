package dataset

import (
    "context";
    "sync";
    "path/filepath";

    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/golang/protobuf/proto";
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

    partitions map[string]Partition
    partitionsMutex *sync.Mutex

    localPartitions utils.Set
}

func newDatasetFromProto(meta *pb.Dataset, ctx context.Context, zk utils.Zookeeper, storage storage.Storage) Dataset {
    return &dataset {
        ctx: ctx,
        meta: meta,
        zk: zk,
        storage: storage,
        partitions: make(map[string]Partition),
        partitionsMutex: &sync.Mutex{},
        localPartitions: utils.NewThreadSafeSet(),
    }
}

func (d *dataset) Meta() *pb.Dataset {
    return d.meta
}

func (d *dataset) LocalPartitions() []interface{} {
    return d.localPartitions.ToSlice()
} 

func (d *dataset) BuildPartitions(updatedPartitions utils.Set) error {
    newPartitions := updatedPartitions.Difference(d.localPartitions)
    if newPartitions.Len() == 0 {
        return nil    
    }
    d.localPartitions = d.localPartitions.Union(updatedPartitions)

    for _, partitionId := range newPartitions.ToSlice() {
        go d.loadPartition(partitionId.(string))   
    }

    return nil
}

func (d *dataset) DeletePartitions(deletedPartitions utils.Set) error {
    d.localPartitions = d.localPartitions.Difference(deletedPartitions)

    for _, partitionId := range deletedPartitions.ToSlice() {
        go d.unloadPartition(partitionId.(string))
    }

    return nil
}

func (d *dataset) DeleteAllPartitions() error {
    for _, partitionId := range d.localPartitions.ToSlice() {
        go d.unloadPartition(partitionId.(string))
    }

    return nil
}

func (d *dataset) loadPartition(partitionId string) {
    partitionMeta, err := d.getPartitionData(partitionId)
    if err != nil {
        panic(err)
    }

    partition := newPartitionFromProto(d.Meta(), partitionMeta, d.ctx, d.zk, d.storage)
    if err := partition.Load(); err != nil {
        panic(err)
    }

    log.Infof("Dataset: `%s`, load partition: %s", d.Meta().GetName(), partitionId)
    d.addPartition(partitionId, partition)
}

func (d *dataset) unloadPartition(partitionId string) {
    partition, exists := d.getPartition(partitionId)
    if !exists {
        panic("Partition does not exists")
    }

    log.Infof("Dataset: `%s`, unload partition: %s", d.Meta().GetName(), partitionId)
    d.deletePartition(partitionId)
    if err := partition.Unload(); err != nil {
        panic(err)
    }
}

func (d *dataset) getPartitionData(partitionId string) (*pb.DatasetPartition, error) {
    partitionData, err := d.zk.Get(filepath.Join(ZkDatasetsPath, d.Meta().GetName(), "partitions", partitionId))
    if err != nil {
        return nil, err
    }

    partitionMeta := &pb.DatasetPartition{}
    if err := proto.Unmarshal(partitionData, partitionMeta); err != nil {
        return nil, err
    }

    return partitionMeta, nil
}

func (d *dataset) getPartition(partitionId string) (Partition, bool) {
    defer d.partitionsMutex.Unlock()
    d.partitionsMutex.Lock()

    partition, exists := d.partitions[partitionId]
    return partition, exists
}

func (d *dataset) addPartition(partitionId string, partition Partition) {
    defer d.partitionsMutex.Unlock()
    d.partitionsMutex.Lock()

    d.partitions[partitionId] = partition   
}

func (d *dataset) deletePartition(partitionId string) {
    defer d.partitionsMutex.Unlock()
    d.partitionsMutex.Lock()

    delete(d.partitions, partitionId)
}
