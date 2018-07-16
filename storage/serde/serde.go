package serde

import (
    "github.com/marekgalovic/tau/math";
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
