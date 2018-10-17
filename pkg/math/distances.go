package math

import (
    pb "github.com/marekgalovic/tau/pkg/protobuf";
)

type Space interface {
    Distance(Vector, Vector) float32
    ToProto() pb.SpaceType
}

type space struct {
    spaceType pb.SpaceType
}

type euclideanSpace struct { space }

type manhattanSpace struct { space }

type cosineSpace struct { space }

func (s *space) ToProto() pb.SpaceType {
    return s.spaceType
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

func NewEuclideanSpace() Space {
    return &euclideanSpace{space{pb.SpaceType_EUCLIDEAN}}
}

func (s *euclideanSpace) Distance(a, b Vector) float32 {
    return EuclideanDistance(a, b)
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
    return &manhattanSpace{space{pb.SpaceType_MANHATTAN}}
}

func (s *manhattanSpace) Distance(a, b Vector) float32 {
    return ManhattanDistance(a, b)
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
    return &cosineSpace{space{pb.SpaceType_COSINE}}
}

func (s *cosineSpace) Distance(a, b Vector) float32 {
    return CosineDistance(a, b)
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
