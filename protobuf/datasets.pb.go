// Code generated by protoc-gen-go. DO NOT EDIT.
// source: datasets.proto

package tau

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type BtreeIndexOptions struct {
	NumTrees     int32 `protobuf:"varint,1,opt,name=num_trees,json=numTrees" json:"num_trees,omitempty"`
	MaxLeafItems int32 `protobuf:"varint,2,opt,name=max_leaf_items,json=maxLeafItems" json:"max_leaf_items,omitempty"`
}

func (m *BtreeIndexOptions) Reset()                    { *m = BtreeIndexOptions{} }
func (m *BtreeIndexOptions) String() string            { return proto.CompactTextString(m) }
func (*BtreeIndexOptions) ProtoMessage()               {}
func (*BtreeIndexOptions) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{0} }

func (m *BtreeIndexOptions) GetNumTrees() int32 {
	if m != nil {
		return m.NumTrees
	}
	return 0
}

func (m *BtreeIndexOptions) GetMaxLeafItems() int32 {
	if m != nil {
		return m.MaxLeafItems
	}
	return 0
}

type VoronoiIndexOptions struct {
	SplitFactor  int32 `protobuf:"varint,1,opt,name=split_factor,json=splitFactor" json:"split_factor,omitempty"`
	MaxCellItems int32 `protobuf:"varint,2,opt,name=max_cell_items,json=maxCellItems" json:"max_cell_items,omitempty"`
}

func (m *VoronoiIndexOptions) Reset()                    { *m = VoronoiIndexOptions{} }
func (m *VoronoiIndexOptions) String() string            { return proto.CompactTextString(m) }
func (*VoronoiIndexOptions) ProtoMessage()               {}
func (*VoronoiIndexOptions) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{1} }

func (m *VoronoiIndexOptions) GetSplitFactor() int32 {
	if m != nil {
		return m.SplitFactor
	}
	return 0
}

func (m *VoronoiIndexOptions) GetMaxCellItems() int32 {
	if m != nil {
		return m.MaxCellItems
	}
	return 0
}

type Index struct {
	Name   string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Size   int32  `protobuf:"varint,2,opt,name=size" json:"size,omitempty"`
	Metric string `protobuf:"bytes,3,opt,name=metric" json:"metric,omitempty"`
	// Types that are valid to be assigned to Options:
	//	*Index_Btree
	//	*Index_Voronoi
	Options isIndex_Options `protobuf_oneof:"options"`
}

func (m *Index) Reset()                    { *m = Index{} }
func (m *Index) String() string            { return proto.CompactTextString(m) }
func (*Index) ProtoMessage()               {}
func (*Index) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{2} }

type isIndex_Options interface {
	isIndex_Options()
}

type Index_Btree struct {
	Btree *BtreeIndexOptions `protobuf:"bytes,4,opt,name=btree,oneof"`
}
type Index_Voronoi struct {
	Voronoi *VoronoiIndexOptions `protobuf:"bytes,5,opt,name=voronoi,oneof"`
}

func (*Index_Btree) isIndex_Options()   {}
func (*Index_Voronoi) isIndex_Options() {}

func (m *Index) GetOptions() isIndex_Options {
	if m != nil {
		return m.Options
	}
	return nil
}

func (m *Index) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Index) GetSize() int32 {
	if m != nil {
		return m.Size
	}
	return 0
}

func (m *Index) GetMetric() string {
	if m != nil {
		return m.Metric
	}
	return ""
}

func (m *Index) GetBtree() *BtreeIndexOptions {
	if x, ok := m.GetOptions().(*Index_Btree); ok {
		return x.Btree
	}
	return nil
}

func (m *Index) GetVoronoi() *VoronoiIndexOptions {
	if x, ok := m.GetOptions().(*Index_Voronoi); ok {
		return x.Voronoi
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*Index) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _Index_OneofMarshaler, _Index_OneofUnmarshaler, _Index_OneofSizer, []interface{}{
		(*Index_Btree)(nil),
		(*Index_Voronoi)(nil),
	}
}

func _Index_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*Index)
	// options
	switch x := m.Options.(type) {
	case *Index_Btree:
		b.EncodeVarint(4<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Btree); err != nil {
			return err
		}
	case *Index_Voronoi:
		b.EncodeVarint(5<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Voronoi); err != nil {
			return err
		}
	case nil:
	default:
		return fmt.Errorf("Index.Options has unexpected type %T", x)
	}
	return nil
}

func _Index_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*Index)
	switch tag {
	case 4: // options.btree
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(BtreeIndexOptions)
		err := b.DecodeMessage(msg)
		m.Options = &Index_Btree{msg}
		return true, err
	case 5: // options.voronoi
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(VoronoiIndexOptions)
		err := b.DecodeMessage(msg)
		m.Options = &Index_Voronoi{msg}
		return true, err
	default:
		return false, nil
	}
}

func _Index_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*Index)
	// options
	switch x := m.Options.(type) {
	case *Index_Btree:
		s := proto.Size(x.Btree)
		n += proto.SizeVarint(4<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *Index_Voronoi:
		s := proto.Size(x.Voronoi)
		n += proto.SizeVarint(5<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type Dataset struct {
	Name  string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Path  string `protobuf:"bytes,2,opt,name=path" json:"path,omitempty"`
	Index *Index `protobuf:"bytes,3,opt,name=index" json:"index,omitempty"`
}

func (m *Dataset) Reset()                    { *m = Dataset{} }
func (m *Dataset) String() string            { return proto.CompactTextString(m) }
func (*Dataset) ProtoMessage()               {}
func (*Dataset) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{3} }

func (m *Dataset) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Dataset) GetPath() string {
	if m != nil {
		return m.Path
	}
	return ""
}

func (m *Dataset) GetIndex() *Index {
	if m != nil {
		return m.Index
	}
	return nil
}

func init() {
	proto.RegisterType((*BtreeIndexOptions)(nil), "tau.BtreeIndexOptions")
	proto.RegisterType((*VoronoiIndexOptions)(nil), "tau.VoronoiIndexOptions")
	proto.RegisterType((*Index)(nil), "tau.Index")
	proto.RegisterType((*Dataset)(nil), "tau.Dataset")
}

func init() { proto.RegisterFile("datasets.proto", fileDescriptor2) }

var fileDescriptor2 = []byte{
	// 304 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x91, 0xc1, 0x6a, 0xf2, 0x40,
	0x10, 0xc7, 0xcd, 0xa7, 0xd1, 0x2f, 0xa3, 0x08, 0xdd, 0x82, 0x04, 0x7a, 0xb1, 0xd2, 0x83, 0xa7,
	0x1c, 0x6c, 0x9f, 0xc0, 0x96, 0xa2, 0x50, 0x28, 0xa4, 0xc5, 0x63, 0xc3, 0xaa, 0x23, 0x5d, 0xc8,
	0xee, 0x86, 0xec, 0xa4, 0x48, 0x9f, 0xac, 0x8f, 0x57, 0x76, 0x36, 0x1e, 0x8a, 0xed, 0x6d, 0xe6,
	0xbf, 0xff, 0xf9, 0xcd, 0xcc, 0x0e, 0x8c, 0xf7, 0x92, 0xa4, 0x43, 0x72, 0x59, 0x55, 0x5b, 0xb2,
	0xa2, 0x4b, 0xb2, 0x99, 0x6d, 0xe0, 0x62, 0x49, 0x35, 0xe2, 0xda, 0xec, 0xf1, 0xf8, 0x5c, 0x91,
	0xb2, 0xc6, 0x89, 0x2b, 0x48, 0x4c, 0xa3, 0x0b, 0xaf, 0xbb, 0x34, 0x9a, 0x46, 0xf3, 0x38, 0xff,
	0x6f, 0x1a, 0xfd, 0xea, 0x73, 0x71, 0x03, 0x63, 0x2d, 0x8f, 0x45, 0x89, 0xf2, 0x50, 0x28, 0x42,
	0xed, 0xd2, 0x7f, 0xec, 0x18, 0x69, 0x79, 0x7c, 0x42, 0x79, 0x58, 0x7b, 0x6d, 0xf6, 0x06, 0x97,
	0x1b, 0x5b, 0x5b, 0x63, 0xd5, 0x0f, 0xf2, 0x35, 0x8c, 0x5c, 0x55, 0x2a, 0x2a, 0x0e, 0x72, 0x47,
	0xb6, 0x6e, 0xe1, 0x43, 0xd6, 0x1e, 0x59, 0x3a, 0xf1, 0x77, 0x58, 0x96, 0x67, 0xfc, 0x7b, 0x2c,
	0xcb, 0xc0, 0xff, 0x8a, 0x20, 0x66, 0xb2, 0x10, 0xd0, 0x33, 0x52, 0x23, 0xa3, 0x92, 0x9c, 0x63,
	0xaf, 0x39, 0xf5, 0x89, 0x6d, 0x25, 0xc7, 0x62, 0x02, 0x7d, 0x8d, 0x54, 0xab, 0x5d, 0xda, 0x65,
	0x67, 0x9b, 0x89, 0x0c, 0xe2, 0xad, 0xdf, 0x34, 0xed, 0x4d, 0xa3, 0xf9, 0x70, 0x31, 0xc9, 0x48,
	0x36, 0xd9, 0xd9, 0x9f, 0xac, 0x3a, 0x79, 0xb0, 0x89, 0x3b, 0x18, 0x7c, 0x84, 0xcd, 0xd2, 0x98,
	0x2b, 0x52, 0xae, 0xf8, 0x65, 0xdb, 0x55, 0x27, 0x3f, 0x59, 0x97, 0x09, 0x0c, 0x6c, 0x50, 0x67,
	0x2f, 0x30, 0x78, 0x08, 0x97, 0xf8, 0x6b, 0xf6, 0x4a, 0xd2, 0x3b, 0xcf, 0x9e, 0xe4, 0x1c, 0x8b,
	0x29, 0xc4, 0xca, 0x83, 0x79, 0xf4, 0xe1, 0x02, 0xb8, 0x23, 0xb7, 0xca, 0xc3, 0xc3, 0xb6, 0xcf,
	0x37, 0xbd, 0xfd, 0x0e, 0x00, 0x00, 0xff, 0xff, 0x9c, 0x91, 0xe1, 0x52, 0xe5, 0x01, 0x00, 0x00,
}
