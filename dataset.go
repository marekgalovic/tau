package tau

import (
    "fmt";
    "bufio";

    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/storage/serde";
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
    nodeManager NodeManager
}

func NewDataset(name string, index index.Index, storage storage.Storage, nodeManager NodeManager) *dataset {
    return &dataset {
        name: name,
        index: index,
        storage: storage,
        nodeManager: nodeManager,
    }
}

func (d *dataset) GetName() string {
    return d.name
}

func (d *dataset) GetIndex() index.Index {
    return d.index
}

func (d *dataset) Load(path string, deserializer serde.Deserializer) error {
    files, err := d.storage.ListFiles(path)
    if err != nil {
        return err
    }

    availableNodes, err := d.nodeManager.GetNodes()
    if err != nil {
        return err
    }

    if len(availableNodes) == 0 {
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
        reader := bufio.NewReader(fileReader)

        for {
            lineBytes, _, err := reader.ReadLine()
            if err != nil {
                break
            }
            id, vector, err := deserializer.DeserializeItem(lineBytes)
            if err != nil {
                return err
            }
            d.index.Add(id, vector)
        }
    }
    return nil
}

func (d *dataset) loadDistributed(files []string, deserializer serde.Deserializer) error {
    return fmt.Errorf("Distributed loading not yet supported")
}
