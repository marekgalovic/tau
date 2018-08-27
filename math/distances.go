package math

import (
    pb "github.com/marekgalovic/tau/protobuf";
)

type Space interface {
    Distance(Vector, Vector) float32
    ToProto() pb.SpaceType
}

type space struct {
    spaceType pb.SpaceType
    distanceFunc func(Vector, Vector) float32
}

func NewSpaceFromProto(spaceType pb.SpaceType) Space {
    switch spaceType {
    case pb.SpaceType_EUCLIDEAN:
        return NewEuclideanSpace()
    case pb.SpaceType_MANHATTAN:
        return NewManhattanSpace()
    case pb.SpaceType_COSINE:
        return NewCosineSpace()
    default:
        panic("Invalid space type")
    }
}

func (s *space) Distance(a, b Vector) float32 {
    return s.distanceFunc(a, b)
}

func (s *space) ToProto() pb.SpaceType {
    return s.spaceType
}

func NewEuclideanSpace() Space {
    return &space{pb.SpaceType_EUCLIDEAN, EuclideanDistance}
}

func EuclideanDistance(a, b Vector) float32 {
    assertSameDim(&a, &b)

    var distance float32
    for i := 0; i < len(a); i++ {
        distance += Square(a[i] - b[i])
    }
    
    return Sqrt(distance)
}

func NewManhattanSpace() Space {
    return &space{pb.SpaceType_MANHATTAN, ManhattanDistance}
}

func ManhattanDistance(a, b Vector) float32 {
    assertSameDim(&a, &b)

    var distance float32
    for i := 0; i < len(a); i++ {
        distance += Abs(a[i] - b[i])
    }

    return distance
}

func NewCosineSpace() Space {
    return &space{pb.SpaceType_COSINE, CosineDistance}
}

func CosineDistance(a, b Vector) float32 {
    assertSameDim(&a, &b)

    var dot float32
    var aNorm float32
    var bNorm float32
    for i := 0; i < len(a); i++ {
        dot += a[i] * b[i]
        aNorm += Square(a[i])
        bNorm += Square(b[i])
    }

    return dot / (Sqrt(aNorm) * Sqrt(bNorm))
}
