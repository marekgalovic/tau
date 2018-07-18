package dataset

import (
    "io";
    "fmt";
    "context";
    "path/filepath";
    "sort";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/cluster";
    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/golang/protobuf/proto";
    log "github.com/Sirupsen/logrus";
)

type Dataset interface {
    Meta() *pb.Dataset
    Search(int32, math.Vector) ([]*pb.SearchResultItem, error)
    SearchPartitions(int32, math.Vector, []string) ([]*pb.SearchResultItem, error)
    LocalPartitions() []interface{}
    BuildPartitions(utils.Set) error
    DeletePartitions(utils.Set) error
    DeleteAllPartitions() error
}

type dataset struct {
    ctx context.Context
    config DatasetManagerConfig
    meta *pb.Dataset
    zk utils.Zookeeper
    cluster cluster.Cluster
    storage storage.Storage

    partitions map[string]Partition
    localPartitions utils.Set
}

func newDatasetFromProto(meta *pb.Dataset, ctx context.Context, config DatasetManagerConfig, zk utils.Zookeeper, cluster cluster.Cluster, storage storage.Storage) (Dataset, error) {
    d := &dataset {
        ctx: ctx,
        config: config,
        meta: meta,
        zk: zk,
        cluster: cluster,
        storage: storage,
        partitions: make(map[string]Partition),
        localPartitions: utils.NewThreadSafeSet(),
    }

    if err := d.loadPartitions(); err != nil {
        return nil, fmt.Errorf("Failed to load dataset partitions. Err: %s", err)
    }

    return d, nil
}

func (d *dataset) loadPartitions() error {
    partitions, err := d.listPartitions()
    if err != nil {
        return err
    }

    for _, partitionId := range partitions {
        partition, err := d.getPartitionData(partitionId)
        if err != nil {
            return err
        }

        d.partitions[partitionId] = newPartitionFromProto(d.Meta(), partition, d.ctx, d.config, d.zk, d.cluster, d.storage)
    }

    return nil
}

func (d *dataset) Meta() *pb.Dataset {
    return d.meta
}

func (d *dataset) Search(k int32, query math.Vector) ([]*pb.SearchResultItem, error) {
    log.Infof("Search k: %d", k)

    nodePartitions := make(map[cluster.Node][]string)
    for partitionId, partition := range d.partitions {
        node, err := partition.GetNode()
        if err != nil {
            return nil, err
        }

        if _, exists := nodePartitions[node]; !exists {
            nodePartitions[node] = make([]string, 0)
        }

        nodePartitions[node] = append(nodePartitions[node], partitionId)
    }

    ctx, cancel := context.WithCancel(d.ctx)
    items := make(chan *pb.SearchResultItem)
    errors := make(chan error)
    for node, partitions := range nodePartitions {
        go d.searchNodePartitions(node, k, query, partitions, ctx, items, errors)
    }

    result := make(index.SearchResult, 0)
    nDone := 0
    SEARCH_LOOP:
    for {
        select {
        case <-d.ctx.Done():
            break SEARCH_LOOP
        case item := <-items:
            if item == nil {
                nDone++
            } else {
                result = append(result, item)
            }
            if nDone == len(nodePartitions) {
                break SEARCH_LOOP
            }
        case err := <-errors:
            cancel()
            close(items)
            close(errors)
            return nil, err
        }
    }
    sort.Sort(result)

    if int(k) > len(result) {
        k = int32(len(result))
    }

    return result[:k], nil 
}

func (d *dataset) SearchPartitions(k int32, query math.Vector, partitions []string) ([]*pb.SearchResultItem, error) {
    log.Infof("Search dataset partitions. Dataset: `%s`, partitions: %s", d.Meta().GetName(), partitions)
    result := make(index.SearchResult, 0)

    for _, partitionId := range partitions {
        partitionIndex := d.partitions[partitionId].Index()
        if partitionIndex == nil {
            return nil, fmt.Errorf("No index found for dataset: `%s`, partition: `%s`", d.Meta().GetName(), partitionId)
        }

        result = append(result, partitionIndex.Search(d.ctx, query)...)
    }
    sort.Sort(result)

    if int(k) > len(result) {
        k = int32(len(result))
    }

    return result[:k], nil
}

func (d *dataset) searchNodePartitions(node cluster.Node, k int32, query math.Vector, partitions []string, ctx context.Context, result chan *pb.SearchResultItem, errors chan error) {
    conn, err := node.Dial()
    if err != nil {
        errors <- err
        return
    }

    stream, err := pb.NewSearchServiceClient(conn).SearchPartitions(d.ctx, &pb.SearchPartitionsRequest{
        DatasetName: d.Meta().GetName(),
        K: k,
        Query: []float32(query),
        Partitions: partitions,
    })

    for {
        if stream == nil {
            break
        }
        
        item, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            errors <- err
            return
        }

        select {
        case <-ctx.Done():
            return
        case result <- item:
        }
    }

    select {
    case <-ctx.Done():
        return
    case result <- nil:
    }
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

    ctx, cancel := context.WithCancel(d.ctx)
    defer cancel()

    errors := make(chan error)
    for _, partitionId := range newPartitions.ToSlice() {
        go d.loadPartition(ctx, partitionId.(string), errors)
    }

    return utils.WaitUntilAllSuccessful(d.ctx, newPartitions.Len(), errors)
}

func (d *dataset) DeletePartitions(deletedPartitions utils.Set) error {
    d.localPartitions = d.localPartitions.Difference(deletedPartitions)

    errors := make(chan error)
    for _, partitionId := range deletedPartitions.ToSlice() {
        go d.unloadPartition(partitionId.(string), errors)
    }

    return utils.WaitUntilAllSuccessful(d.ctx, deletedPartitions.Len(), errors)
}

func (d *dataset) DeleteAllPartitions() error {
    errors := make(chan error)
    for _, partitionId := range d.localPartitions.ToSlice() {
        go d.unloadPartition(partitionId.(string), errors)
    }

    return utils.WaitUntilAllSuccessful(d.ctx, d.localPartitions.Len(), errors)
}

func (d *dataset) loadPartition(ctx context.Context, partitionId string, errors chan error) {
    partition := d.partitions[partitionId]

    errors <- partition.Load(ctx)
}

func (d *dataset) unloadPartition(partitionId string, errors chan error) {
    partition, exists := d.partitions[partitionId]
    if !exists {
        errors <- fmt.Errorf("Partition %d does not exist", partitionId)
    }

    errors <- partition.Unload()
}

func (d *dataset) listPartitions() ([]string, error) {
    return d.zk.Children(filepath.Join(ZkDatasetsPath, d.Meta().GetName(), "partitions"))
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
