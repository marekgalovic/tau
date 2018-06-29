package tau

import (
    "fmt";
    "os";
    "bufio";
    "strings";
    "crypto/sha256";

    "github.com/marekgalovic/tau/math";
    "github.com/marekgalovic/tau/index";
    "github.com/marekgalovic/tau/storage";
    "github.com/marekgalovic/tau/storage/serde";
    pb "github.com/marekgalovic/tau/protobuf";
)

type Dataset interface {
    Load(string) error
}

type dataset struct {
    meta *pb.Dataset
    index index.Index
    storage storage.Storage
}

func NewDataset(name, path string, index index.Index, storage storage.Storage) *dataset {
    return &dataset {
        meta: &pb.Dataset{
            Name: name,
            Path: path,
            Index: index.ToProto(),
        },
        index: index,
        storage: storage,
    }
}

func (d *dataset) Search(k int, query math.Vector) ([]*pb.SearchResultItem, error) {
    return nil, nil
}

func (d *dataset) Load(path string, deserializer serde.Deserializer) error {
    files, err := d.storage.ListFiles(path)
    if err != nil {
        return err
    }

    return d.loadLocal(files, deserializer)
}

func (d *dataset) loadLocal(files []string, deserializer serde.Deserializer) error {
    if err := d.populateIndex(files, deserializer); err != nil {
        return err
    }

    d.index.Build()

    indexFile, err := os.Create(fmt.Sprintf("/tmp/%s.tau_index", d.indexFileName(files)))
    if err != nil {
        return err
    }
    if err := d.index.Save(indexFile); err != nil {
        return err
    }

    return indexFile.Close()
}

func (d *dataset) populateIndex(files []string, deserializer serde.Deserializer) error {
    for _, filePath := range files {
        fileReader, err := d.storage.Reader(filePath)
        if err != nil {
            return err
        }

        bufReader := bufio.NewReader(fileReader)
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
    }
    return nil
}

func (d *dataset) indexFileName(files []string) string {
    names := strings.Join(append(files, d.meta.Name), "")

    return fmt.Sprintf("%x", sha256.Sum256([]byte(names)))
}
