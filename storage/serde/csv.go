package serde

import (
    "fmt";
    "io";
    "encoding/csv";
    "strconv";

    "github.com/marekgalovic/tau/math";
)

var csvSeparator []byte = []byte(",")

type csvReader struct{
    reader *csv.Reader
}

func NewCsvReader(file io.Reader) *csvReader {
    return &csvReader{
        reader: csv.NewReader(file),
    }
}

func (r *csvReader) ReadItem() (int64, math.Vector, error) {
    line, err := r.reader.Read()
    if err != nil {
        return 0, nil, err
    }

    return r.deserialize(line)
}

func (r *csvReader) deserialize(data []string) (int64, math.Vector, error) {
    if len(data) < 2 {
        return 0, nil, fmt.Errorf("Not enough values")
    }

    id, err := strconv.ParseInt(data[0], 10, 64)
    if err != nil {
        return 0, nil, err
    }

    vec := make(math.Vector, len(data[1:]))
    for i, rawValue := range data[1:] {
        value, err := strconv.ParseFloat(rawValue, 32)
        if err != nil {
            return 0, nil, err
        }
        vec[i] = math.Float(float32(value))
    }

    return id, vec, nil
}
