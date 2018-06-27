package serde

import (
    "github.com/marekgalovic/tau/math";
)

type Serializer interface {
    SerializeItem(int, math.Vector) ([]byte, error)
}

type Deserializer interface {
    DeserializeItem([]byte) (int, math.Vector, error)
}

type SerializerDeserializer interface {
    Serializer
    Deserializer
}
