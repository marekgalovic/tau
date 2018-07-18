package serde

import (
    "io";
    "fmt";

    "github.com/marekgalovic/tau/math";
    pb "github.com/marekgalovic/tau/protobuf";
)

type Serializer interface {
    SerializeItem(int64, math.Vector) ([]byte, error)
}

type Deserializer interface {
    ReadItem() (int64, math.Vector, error)
}

type SerializerDeserializer interface {
    Serializer
    Deserializer
}

type indexItem struct {
    Id int64 `json:"id"`
    Value []float32 `json:"value"`   
}

func NewReaderFromProto(format interface{}, reader io.Reader) (Deserializer, error) {
    switch format.(type) {
    case *pb.Dataset_Csv:
        return NewCsvReader(reader), nil
    case *pb.Dataset_Json:
        return NewJsonReader(reader), nil
    default:
        return nil, fmt.Errorf("Invalid dataset format")
    }
}
