package serde

import (
    "fmt";
    "io";
    "bytes";
    "bufio";
    "encoding/binary";

    "github.com/marekgalovic/tau/math";
)

type csvReader struct{
    reader *bufio.Reader
    sep []byte
}

func NewCsvReader(file io.Reader, sep string) *csvReader {
    return &csvReader{
        reader: bufio.NewReader(file),
        sep: []byte(sep),
    }
}

func (r *csvReader) ReadItem() (int64, math.Vector, error) {
    line, _, err := r.reader.ReadLine()
    if err != nil {
        return 0, nil, err
    }

    return r.deserialize(line)
}

func (r *csvReader) deserialize(data []byte) (int64, math.Vector, error) {
    values := bytes.Split(data, r.sep)
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
