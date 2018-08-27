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
    Search(context.Context, int, math.Vector) SearchResult
    ByteSize() int
    Len() int
    Size() int
    Reset()
    Items() map[int64]math.Vector
    Add(int64, math.Vector) error
    Get(int64) math.Vector
    Load(io.Reader) error
    Save(io.Writer) error
    ToProto() *pb.Index
}

type baseIndex struct {
    size int
    space math.Space
    items map[int64]math.Vector
}

func newBaseIndex(size int, space math.Space) baseIndex {
    return baseIndex {
        size: size,
        space: space,
        items: make(map[int64]math.Vector),
    }
}

func (i *baseIndex) ByteSize() int {
    return 4 * int(i.size) * i.Len()
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
        Space: i.space.ToProto(),
    }
}

func FromProto(proto *pb.Index) Index {
    switch options := proto.Options.(type) {
    case *pb.Index_Voronoi:
        return NewVoronoiIndex(int(proto.Size), math.NewSpaceFromProto(proto.Space), VoronoiSplitFactor(int(options.Voronoi.SplitFactor)), VoronoiMaxCellItems(int(options.Voronoi.MaxCellItems)))
    case *pb.Index_Btree:
        return NewBtreeIndex(int(proto.Size), math.NewSpaceFromProto(proto.Space), BtreeNumTrees(int(options.Btree.NumTrees)), BtreeMaxLeafItems(int(options.Btree.MaxLeafItems)))
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
    if err := binary.Write(writer, binary.LittleEndian, int32(index.space.ToProto())); err != nil {
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
    var spaceEnum pb.SpaceType
    if err := binary.Read(reader, binary.LittleEndian, &spaceEnum); err != nil {
        return nil, err
    }
    index.space = math.NewSpaceFromProto(spaceEnum)

    return indexType, nil
}
