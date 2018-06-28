package tau

import (
    "fmt";
    "io";
    "bufio";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/storage/serde";
    pb "github.com/marekgalovic/tau/protobuf";
)

type Dataset interface {
    GetName() string
    GetIndex() index.Index
    Load(string) error
}

type dataset struct {
    name string
    index index.Index
    storage storage.Storage
    cluster Cluster
}

func NewDataset(name string, index index.Index, storage storage.Storage, cluster Cluster) *dataset {
    return &dataset {
        name: name,
        index: index,
        storage: storage,
        cluster: cluster,
    }
}

func (d *dataset) GetName() string {
    return d.name
}

func (d *dataset) GetIndex() index.Index {
    return d.index
}

func (d *dataset) Search(k int, query math.Vector) ([]*pb.SearchResultItem, error) {
    return nil, nil
}

func (d *dataset) Load(path string, deserializer serde.Deserializer) error {
    files, err := d.storage.ListFiles(path)
    if err != nil {
        return err
    }

    nodesCount, err := d.cluster.NodesCount()
    if err != nil {
        return err
    }

    if nodesCount == 0 {
        return d.loadLocal(files, deserializer)
    }
    return d.loadDistributed(files, deserializer)
}

func (d *dataset) loadLocal(files []string, deserializer serde.Deserializer) error {
    for _, filePath := range files {
        fileReader, err := d.storage.Reader(filePath)
        if err != nil {
            return err
        }

        if err = d.populateIndex(fileReader, deserializer); err != nil {
            return err
        }
    }
    return nil
}

func (d *dataset) loadDistributed(files []string, deserializer serde.Deserializer) error {  
    return fmt.Errorf("Distributed loading not yet supported")
}

func (d *dataset) populateIndex(reader io.Reader, deserializer serde.Deserializer) error {
    bufReader := bufio.NewReader(reader)

    for {
        lineBytes, _, err := bufReader.ReadLine()
        if err != nil {
            break
        }
        id, vector, err := deserializer.DeserializeItem(lineBytes)
        if err != nil {
            return err
        }
        d.index.Add(id, vector)
    }

    return nil
}
