package serde

import (
    "fmt";
    "bytes";
    "encoding/binary";

    "github.com/marekgalovic/tau/math";
)

type csvSerde struct{
    sep []byte
}

func NewCsv(sep string) *csvSerde {
    return &csvSerde{
        sep: []byte(sep),
    }
}

func (csv *csvSerde) DeserializeItem(data []byte) (int64, math.Vector, error) {
    values := bytes.Split(data, csv.sep)
    if len(values) < 2 {
        return 0, nil, fmt.Errorf("Not enough values")
    }

    id, nRead := binary.Varint(values[0])
    if nRead == 0 {
        return 0, nil, fmt.Errorf("Failed to parse item id. Buffer to small")
    }
    if nRead < 0 {
        return 0, nil, fmt.Errorf("Item id value is greater that int64. Buffer overflow")
    }

    vector, err := math.VectorFromBytes(values[1:])
    if err != nil {
        return 0, nil, err
    }
    return id, vector, nil
}
