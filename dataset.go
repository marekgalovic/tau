package tau

import (
    "fmt";
    // "bufio";
    "strings";
    "crypto/sha256";
    "sync";

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
    meta *pb.Dataset
    index *pb.Index

    partitions utils.Set
    partitionIndices map[string]index.Index
    partitionIndicesMutex *sync.Mutex

    zk *zk.Conn
    storage storage.Storage
}

func newDatasetFromProto(proto *pb.Dataset, zkConn *zk.Conn, storage storage.Storage) Dataset {
    return &dataset {
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

func (d *dataset) BuildPartitions(partitions utils.Set) error {
    newPartitions := partitions.Difference(d.partitions)
    if newPartitions.Len() == 0 {
        return nil    
    }

    log.Infof("Dataset: %s, partitions: %s, new partitions: %s", d.Meta().GetName(), d.partitions, newPartitions)
    d.partitions = d.partitions.Union(partitions)
    return nil
}

func (d *dataset) DeletePartitions(partitions utils.Set) error {
    d.partitions = d.partitions.Difference(partitions)

    log.Infof("Release dataset: %s, partitions: %s, released partitions: %s", d.Meta().GetName(), d.partitions, partitions)
    return nil
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

func (d *dataset) indexFileName(files []string) string {
    names := strings.Join(append(files, d.meta.Name), "")

    return fmt.Sprintf("%x", sha256.Sum256([]byte(names)))
}
