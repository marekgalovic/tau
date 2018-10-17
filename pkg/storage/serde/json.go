package serde

import (
    "io";
    "bufio";
    "encoding/json";

    "github.com/marekgalovic/tau/pkg/math";
)

type jsonReader struct {
    reader *bufio.Reader
}

func NewJsonReader(file io.Reader) *jsonReader {
    return &jsonReader {
        reader: bufio.NewReader(file),
    }
}

func (r *jsonReader) ReadItem() (int64, math.Vector, error) {
    line, err := r.readFullLine()
    if err != nil {
        return 0, nil, err
    }

    item := indexItem{}
    if err := json.Unmarshal(line, &item); err != nil {
        return 0, nil, err
    }

    return item.Id, math.Vector(item.Value), nil
}

func (r *jsonReader) readFullLine() ([]byte, error) {
    line := make([]byte, 0)

    isPrefix := true
    for isPrefix {
        var bytes []byte
        var err error

        bytes, isPrefix, err = r.reader.ReadLine()
        if err != nil {
            return nil, err
        }

        line = append(line, bytes...)
    }

    return line, nil
}
