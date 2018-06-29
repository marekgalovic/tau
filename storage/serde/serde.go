package serde

import (
    "github.com/marekgalovic/tau/math";
)

type Serializer interface {
    SerializeItem(int64, math.Vector) ([]byte, error)
}

type Deserializer interface {
    DeserializeItem([]byte) (int64, math.Vector, error)
}

type SerializerDeserializer interface {
    Serializer
    Deserializer
}
