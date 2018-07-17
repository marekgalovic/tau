 package index

import (
    "fmt";
    "io";
    "context";
    "encoding/binary";

    "github.com/marekgalovic/tau/math";
    pb "github.com/marekgalovic/tau/protobuf"
)

type Index interface {
    Build(context.Context)
    Search(context.Context, math.Vector) SearchResult
    ByteSize() int
    Len() int
    Size() int
    Reset()
    Items() map[int64]math.Vector
    Add(int64, math.Vector) error
    Get(int64) math.Vector
    ComputeDistance(math.Vector, math.Vector) float32
    Load(io.Reader) error
    Save(io.Writer) error
    ToProto() *pb.Index
}

type baseIndex struct {
    size int
    metric string
    items map[int64]math.Vector
}

func newBaseIndex(size int, metric string) baseIndex {
    return baseIndex{
        size: size,
        metric: metric,
        items: make(map[int64]math.Vector),
    }
}

func (i *baseIndex) ByteSize() int {
    return 8 * int(i.size) * i.Len()
}

func (i *baseIndex) Len() int {
    return len(i.items)
}

func (i *baseIndex) Size() int {
    return i.size
}

func (i *baseIndex) Reset() {
    i.items = make(map[int64]math.Vector)
}

func (i *baseIndex) Items() map[int64]math.Vector {
    return i.items
}

func (i *baseIndex) ToProto() *pb.Index {
    return &pb.Index {
        Size: int32(i.size),
        Metric: i.metric,
    }
}

func FromProto(proto *pb.Index) Index {
    switch options := proto.Options.(type) {
    case *pb.Index_Voronoi:
        return NewVoronoiIndex(int(proto.Size), proto.Metric, int(options.Voronoi.SplitFactor), int(options.Voronoi.MaxCellItems))
    case *pb.Index_Btree:
        return NewBtreeIndex(int(proto.Size), proto.Metric, int(options.Btree.NumTrees), int(options.Btree.MaxLeafItems))
    default:
        panic("Invalid index type")
    }
}

func (i *baseIndex) Add(id int64, vec math.Vector) error {
    if len(vec) != i.size {
        return fmt.Errorf("Vector with %d components does not match index size %d", len(vec), i.size)
    }
    if _, exists := i.items[id]; exists {
        return fmt.Errorf("Id: %d already exists", id)
    }

    i.items[id] = vec
    return nil
}

func (i *baseIndex) Get(id int64) math.Vector {
    return i.items[id]
}

func (i *baseIndex) ComputeDistance(a, b math.Vector) float32 {
    if i.metric == "Euclidean" {
        return math.EuclideanDistance(a, b)
    }
    if i.metric == "Manhattan" {
        return math.ManhattanDistance(a, b)
    }
    if i.metric == "Cosine" {
        return math.CosineDistance(a, b)
    }
    panic("Invalid metric")
}

func (index *baseIndex) writeHeader(writer io.Writer, indexType []byte) error {
    if len(indexType) != 6 {
        return fmt.Errorf("Index type must be 6 bytes long")
    }
    // Index type
    if _, err := writer.Write(indexType); err != nil {
        return err
    }
    // Index size
    if err := binary.Write(writer, binary.LittleEndian, int32(index.size)); err != nil {
        return err
    }
    // Metric
    metricBytes := []byte(index.metric)
    if err := binary.Write(writer, binary.LittleEndian, int32(len(metricBytes))); err != nil {
        return err
    }
    if _, err := writer.Write(metricBytes); err != nil {
        return err
    }

    return nil
}

func (index *baseIndex) readHeader(reader io.Reader) ([]byte, error) {
    // Index type
    indexType := make([]byte, 6)
    if _, err := reader.Read(indexType); err != nil {
        return nil, err
    }
    // Index size
    var size int32
    if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
        return nil, err
    }
    index.size = int(size)
    // Metric
    var metricBytesSize int32
    if err := binary.Read(reader, binary.LittleEndian, &metricBytesSize); err != nil {
        return nil, err
    }
    metric := make([]byte, metricBytesSize)
    if _, err := reader.Read(metric); err != nil {
        return nil, err
    }
    index.metric = string(metric)

    return indexType, nil
}
