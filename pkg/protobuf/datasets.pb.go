// Code generated by protoc-gen-go. DO NOT EDIT.
// source: datasets.proto

package tau

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type HnswSearchAlgorithm int32

const (
	HnswSearchAlgorithm_SIMPLE    HnswSearchAlgorithm = 0
	HnswSearchAlgorithm_HEURISTIC HnswSearchAlgorithm = 1
)

var HnswSearchAlgorithm_name = map[int32]string{
	0: "SIMPLE",
	1: "HEURISTIC",
}
var HnswSearchAlgorithm_value = map[string]int32{
	"SIMPLE":    0,
	"HEURISTIC": 1,
}

func (x HnswSearchAlgorithm) String() string {
	return proto.EnumName(HnswSearchAlgorithm_name, int32(x))
}
func (HnswSearchAlgorithm) EnumDescriptor() ([]byte, []int) { return fileDescriptor2, []int{0} }

type GetDatasetRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
}

func (m *GetDatasetRequest) Reset()                    { *m = GetDatasetRequest{} }
func (m *GetDatasetRequest) String() string            { return proto.CompactTextString(m) }
func (*GetDatasetRequest) ProtoMessage()               {}
func (*GetDatasetRequest) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{0} }

func (m *GetDatasetRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type CreateDatasetRequest struct {
	Dataset *Dataset `protobuf:"bytes,1,opt,name=dataset" json:"dataset,omitempty"`
}

func (m *CreateDatasetRequest) Reset()                    { *m = CreateDatasetRequest{} }
func (m *CreateDatasetRequest) String() string            { return proto.CompactTextString(m) }
func (*CreateDatasetRequest) ProtoMessage()               {}
func (*CreateDatasetRequest) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{1} }

func (m *CreateDatasetRequest) GetDataset() *Dataset {
	if m != nil {
		return m.Dataset
	}
	return nil
}

type CreateDatasetWithPartitionsRequest struct {
	Dataset    *Dataset            `protobuf:"bytes,1,opt,name=dataset" json:"dataset,omitempty"`
	Partitions []*DatasetPartition `protobuf:"bytes,2,rep,name=partitions" json:"partitions,omitempty"`
}

func (m *CreateDatasetWithPartitionsRequest) Reset()         { *m = CreateDatasetWithPartitionsRequest{} }
func (m *CreateDatasetWithPartitionsRequest) String() string { return proto.CompactTextString(m) }
func (*CreateDatasetWithPartitionsRequest) ProtoMessage()    {}
func (*CreateDatasetWithPartitionsRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor2, []int{2}
}

func (m *CreateDatasetWithPartitionsRequest) GetDataset() *Dataset {
	if m != nil {
		return m.Dataset
	}
	return nil
}

func (m *CreateDatasetWithPartitionsRequest) GetPartitions() []*DatasetPartition {
	if m != nil {
		return m.Partitions
	}
	return nil
}

type DeleteDatasetRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
}

func (m *DeleteDatasetRequest) Reset()                    { *m = DeleteDatasetRequest{} }
func (m *DeleteDatasetRequest) String() string            { return proto.CompactTextString(m) }
func (*DeleteDatasetRequest) ProtoMessage()               {}
func (*DeleteDatasetRequest) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{3} }

func (m *DeleteDatasetRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type ListPartitionsRequest struct {
	DatasetName string `protobuf:"bytes,1,opt,name=dataset_name,json=datasetName" json:"dataset_name,omitempty"`
}

func (m *ListPartitionsRequest) Reset()                    { *m = ListPartitionsRequest{} }
func (m *ListPartitionsRequest) String() string            { return proto.CompactTextString(m) }
func (*ListPartitionsRequest) ProtoMessage()               {}
func (*ListPartitionsRequest) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{4} }

func (m *ListPartitionsRequest) GetDatasetName() string {
	if m != nil {
		return m.DatasetName
	}
	return ""
}

type BtreeIndexOptions struct {
	NumTrees     int32 `protobuf:"varint,1,opt,name=num_trees,json=numTrees" json:"num_trees,omitempty"`
	MaxLeafItems int32 `protobuf:"varint,2,opt,name=max_leaf_items,json=maxLeafItems" json:"max_leaf_items,omitempty"`
}

func (m *BtreeIndexOptions) Reset()                    { *m = BtreeIndexOptions{} }
func (m *BtreeIndexOptions) String() string            { return proto.CompactTextString(m) }
func (*BtreeIndexOptions) ProtoMessage()               {}
func (*BtreeIndexOptions) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{5} }

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
func (*VoronoiIndexOptions) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{6} }

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

type HnswIndexOptions struct {
	SearchAlgorithm HnswSearchAlgorithm `protobuf:"varint,1,opt,name=search_algorithm,json=searchAlgorithm,enum=tau.HnswSearchAlgorithm" json:"search_algorithm,omitempty"`
	LevelMultiplier float32             `protobuf:"fixed32,2,opt,name=level_multiplier,json=levelMultiplier" json:"level_multiplier,omitempty"`
	Ef              int32               `protobuf:"varint,3,opt,name=ef" json:"ef,omitempty"`
	EfConstruction  int32               `protobuf:"varint,4,opt,name=ef_construction,json=efConstruction" json:"ef_construction,omitempty"`
	M               int32               `protobuf:"varint,5,opt,name=m" json:"m,omitempty"`
	MMax            int32               `protobuf:"varint,6,opt,name=m_max,json=mMax" json:"m_max,omitempty"`
	MMax_0          int32               `protobuf:"varint,7,opt,name=m_max_0,json=mMax0" json:"m_max_0,omitempty"`
}

func (m *HnswIndexOptions) Reset()                    { *m = HnswIndexOptions{} }
func (m *HnswIndexOptions) String() string            { return proto.CompactTextString(m) }
func (*HnswIndexOptions) ProtoMessage()               {}
func (*HnswIndexOptions) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{7} }

func (m *HnswIndexOptions) GetSearchAlgorithm() HnswSearchAlgorithm {
	if m != nil {
		return m.SearchAlgorithm
	}
	return HnswSearchAlgorithm_SIMPLE
}

func (m *HnswIndexOptions) GetLevelMultiplier() float32 {
	if m != nil {
		return m.LevelMultiplier
	}
	return 0
}

func (m *HnswIndexOptions) GetEf() int32 {
	if m != nil {
		return m.Ef
	}
	return 0
}

func (m *HnswIndexOptions) GetEfConstruction() int32 {
	if m != nil {
		return m.EfConstruction
	}
	return 0
}

func (m *HnswIndexOptions) GetM() int32 {
	if m != nil {
		return m.M
	}
	return 0
}

func (m *HnswIndexOptions) GetMMax() int32 {
	if m != nil {
		return m.MMax
	}
	return 0
}

func (m *HnswIndexOptions) GetMMax_0() int32 {
	if m != nil {
		return m.MMax_0
	}
	return 0
}

type Index struct {
	Size  int32     `protobuf:"varint,1,opt,name=size" json:"size,omitempty"`
	Space SpaceType `protobuf:"varint,2,opt,name=space,enum=tau.SpaceType" json:"space,omitempty"`
	// Types that are valid to be assigned to Options:
	//	*Index_Btree
	//	*Index_Voronoi
	//	*Index_Hnsw
	Options isIndex_Options `protobuf_oneof:"options"`
}

func (m *Index) Reset()                    { *m = Index{} }
func (m *Index) String() string            { return proto.CompactTextString(m) }
func (*Index) ProtoMessage()               {}
func (*Index) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{8} }

type isIndex_Options interface {
	isIndex_Options()
}

type Index_Btree struct {
	Btree *BtreeIndexOptions `protobuf:"bytes,3,opt,name=btree,oneof"`
}
type Index_Voronoi struct {
	Voronoi *VoronoiIndexOptions `protobuf:"bytes,4,opt,name=voronoi,oneof"`
}
type Index_Hnsw struct {
	Hnsw *HnswIndexOptions `protobuf:"bytes,5,opt,name=hnsw,oneof"`
}

func (*Index_Btree) isIndex_Options()   {}
func (*Index_Voronoi) isIndex_Options() {}
func (*Index_Hnsw) isIndex_Options()    {}

func (m *Index) GetOptions() isIndex_Options {
	if m != nil {
		return m.Options
	}
	return nil
}

func (m *Index) GetSize() int32 {
	if m != nil {
		return m.Size
	}
	return 0
}

func (m *Index) GetSpace() SpaceType {
	if m != nil {
		return m.Space
	}
	return SpaceType_EUCLIDEAN
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

func (m *Index) GetHnsw() *HnswIndexOptions {
	if x, ok := m.GetOptions().(*Index_Hnsw); ok {
		return x.Hnsw
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*Index) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _Index_OneofMarshaler, _Index_OneofUnmarshaler, _Index_OneofSizer, []interface{}{
		(*Index_Btree)(nil),
		(*Index_Voronoi)(nil),
		(*Index_Hnsw)(nil),
	}
}

func _Index_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*Index)
	// options
	switch x := m.Options.(type) {
	case *Index_Btree:
		b.EncodeVarint(3<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Btree); err != nil {
			return err
		}
	case *Index_Voronoi:
		b.EncodeVarint(4<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Voronoi); err != nil {
			return err
		}
	case *Index_Hnsw:
		b.EncodeVarint(5<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Hnsw); err != nil {
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
	case 3: // options.btree
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(BtreeIndexOptions)
		err := b.DecodeMessage(msg)
		m.Options = &Index_Btree{msg}
		return true, err
	case 4: // options.voronoi
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(VoronoiIndexOptions)
		err := b.DecodeMessage(msg)
		m.Options = &Index_Voronoi{msg}
		return true, err
	case 5: // options.hnsw
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(HnswIndexOptions)
		err := b.DecodeMessage(msg)
		m.Options = &Index_Hnsw{msg}
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
		n += proto.SizeVarint(3<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *Index_Voronoi:
		s := proto.Size(x.Voronoi)
		n += proto.SizeVarint(4<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *Index_Hnsw:
		s := proto.Size(x.Hnsw)
		n += proto.SizeVarint(5<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type CsvDataset struct {
}

func (m *CsvDataset) Reset()                    { *m = CsvDataset{} }
func (m *CsvDataset) String() string            { return proto.CompactTextString(m) }
func (*CsvDataset) ProtoMessage()               {}
func (*CsvDataset) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{9} }

type JsonDataset struct {
}

func (m *JsonDataset) Reset()                    { *m = JsonDataset{} }
func (m *JsonDataset) String() string            { return proto.CompactTextString(m) }
func (*JsonDataset) ProtoMessage()               {}
func (*JsonDataset) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{10} }

type Dataset struct {
	Name          string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Path          string `protobuf:"bytes,2,opt,name=path" json:"path,omitempty"`
	NumPartitions int32  `protobuf:"varint,3,opt,name=num_partitions,json=numPartitions" json:"num_partitions,omitempty"`
	NumReplicas   int32  `protobuf:"varint,4,opt,name=num_replicas,json=numReplicas" json:"num_replicas,omitempty"`
	Index         *Index `protobuf:"bytes,5,opt,name=index" json:"index,omitempty"`
	// Types that are valid to be assigned to Format:
	//	*Dataset_Csv
	//	*Dataset_Json
	Format isDataset_Format `protobuf_oneof:"format"`
}

func (m *Dataset) Reset()                    { *m = Dataset{} }
func (m *Dataset) String() string            { return proto.CompactTextString(m) }
func (*Dataset) ProtoMessage()               {}
func (*Dataset) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{11} }

type isDataset_Format interface {
	isDataset_Format()
}

type Dataset_Csv struct {
	Csv *CsvDataset `protobuf:"bytes,6,opt,name=csv,oneof"`
}
type Dataset_Json struct {
	Json *JsonDataset `protobuf:"bytes,7,opt,name=json,oneof"`
}

func (*Dataset_Csv) isDataset_Format()  {}
func (*Dataset_Json) isDataset_Format() {}

func (m *Dataset) GetFormat() isDataset_Format {
	if m != nil {
		return m.Format
	}
	return nil
}

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

func (m *Dataset) GetNumPartitions() int32 {
	if m != nil {
		return m.NumPartitions
	}
	return 0
}

func (m *Dataset) GetNumReplicas() int32 {
	if m != nil {
		return m.NumReplicas
	}
	return 0
}

func (m *Dataset) GetIndex() *Index {
	if m != nil {
		return m.Index
	}
	return nil
}

func (m *Dataset) GetCsv() *CsvDataset {
	if x, ok := m.GetFormat().(*Dataset_Csv); ok {
		return x.Csv
	}
	return nil
}

func (m *Dataset) GetJson() *JsonDataset {
	if x, ok := m.GetFormat().(*Dataset_Json); ok {
		return x.Json
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*Dataset) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _Dataset_OneofMarshaler, _Dataset_OneofUnmarshaler, _Dataset_OneofSizer, []interface{}{
		(*Dataset_Csv)(nil),
		(*Dataset_Json)(nil),
	}
}

func _Dataset_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*Dataset)
	// format
	switch x := m.Format.(type) {
	case *Dataset_Csv:
		b.EncodeVarint(6<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Csv); err != nil {
			return err
		}
	case *Dataset_Json:
		b.EncodeVarint(7<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Json); err != nil {
			return err
		}
	case nil:
	default:
		return fmt.Errorf("Dataset.Format has unexpected type %T", x)
	}
	return nil
}

func _Dataset_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*Dataset)
	switch tag {
	case 6: // format.csv
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(CsvDataset)
		err := b.DecodeMessage(msg)
		m.Format = &Dataset_Csv{msg}
		return true, err
	case 7: // format.json
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(JsonDataset)
		err := b.DecodeMessage(msg)
		m.Format = &Dataset_Json{msg}
		return true, err
	default:
		return false, nil
	}
}

func _Dataset_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*Dataset)
	// format
	switch x := m.Format.(type) {
	case *Dataset_Csv:
		s := proto.Size(x.Csv)
		n += proto.SizeVarint(6<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *Dataset_Json:
		s := proto.Size(x.Json)
		n += proto.SizeVarint(7<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type DatasetPartition struct {
	Id    int32    `protobuf:"varint,1,opt,name=id" json:"id,omitempty"`
	Files []string `protobuf:"bytes,2,rep,name=files" json:"files,omitempty"`
}

func (m *DatasetPartition) Reset()                    { *m = DatasetPartition{} }
func (m *DatasetPartition) String() string            { return proto.CompactTextString(m) }
func (*DatasetPartition) ProtoMessage()               {}
func (*DatasetPartition) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{12} }

func (m *DatasetPartition) GetId() int32 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *DatasetPartition) GetFiles() []string {
	if m != nil {
		return m.Files
	}
	return nil
}

func init() {
	proto.RegisterType((*GetDatasetRequest)(nil), "tau.GetDatasetRequest")
	proto.RegisterType((*CreateDatasetRequest)(nil), "tau.CreateDatasetRequest")
	proto.RegisterType((*CreateDatasetWithPartitionsRequest)(nil), "tau.CreateDatasetWithPartitionsRequest")
	proto.RegisterType((*DeleteDatasetRequest)(nil), "tau.DeleteDatasetRequest")
	proto.RegisterType((*ListPartitionsRequest)(nil), "tau.ListPartitionsRequest")
	proto.RegisterType((*BtreeIndexOptions)(nil), "tau.BtreeIndexOptions")
	proto.RegisterType((*VoronoiIndexOptions)(nil), "tau.VoronoiIndexOptions")
	proto.RegisterType((*HnswIndexOptions)(nil), "tau.HnswIndexOptions")
	proto.RegisterType((*Index)(nil), "tau.Index")
	proto.RegisterType((*CsvDataset)(nil), "tau.CsvDataset")
	proto.RegisterType((*JsonDataset)(nil), "tau.JsonDataset")
	proto.RegisterType((*Dataset)(nil), "tau.Dataset")
	proto.RegisterType((*DatasetPartition)(nil), "tau.DatasetPartition")
	proto.RegisterEnum("tau.HnswSearchAlgorithm", HnswSearchAlgorithm_name, HnswSearchAlgorithm_value)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for DatasetsService service

type DatasetsServiceClient interface {
	List(ctx context.Context, in *EmptyRequest, opts ...grpc.CallOption) (DatasetsService_ListClient, error)
	Get(ctx context.Context, in *GetDatasetRequest, opts ...grpc.CallOption) (*Dataset, error)
	Create(ctx context.Context, in *CreateDatasetRequest, opts ...grpc.CallOption) (*EmptyResponse, error)
	CreateWithPartitions(ctx context.Context, in *CreateDatasetWithPartitionsRequest, opts ...grpc.CallOption) (*EmptyResponse, error)
	Delete(ctx context.Context, in *DeleteDatasetRequest, opts ...grpc.CallOption) (*EmptyResponse, error)
	ListPartitions(ctx context.Context, in *ListPartitionsRequest, opts ...grpc.CallOption) (DatasetsService_ListPartitionsClient, error)
}

type datasetsServiceClient struct {
	cc *grpc.ClientConn
}

func NewDatasetsServiceClient(cc *grpc.ClientConn) DatasetsServiceClient {
	return &datasetsServiceClient{cc}
}

func (c *datasetsServiceClient) List(ctx context.Context, in *EmptyRequest, opts ...grpc.CallOption) (DatasetsService_ListClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_DatasetsService_serviceDesc.Streams[0], c.cc, "/tau.DatasetsService/List", opts...)
	if err != nil {
		return nil, err
	}
	x := &datasetsServiceListClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DatasetsService_ListClient interface {
	Recv() (*Dataset, error)
	grpc.ClientStream
}

type datasetsServiceListClient struct {
	grpc.ClientStream
}

func (x *datasetsServiceListClient) Recv() (*Dataset, error) {
	m := new(Dataset)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *datasetsServiceClient) Get(ctx context.Context, in *GetDatasetRequest, opts ...grpc.CallOption) (*Dataset, error) {
	out := new(Dataset)
	err := grpc.Invoke(ctx, "/tau.DatasetsService/Get", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *datasetsServiceClient) Create(ctx context.Context, in *CreateDatasetRequest, opts ...grpc.CallOption) (*EmptyResponse, error) {
	out := new(EmptyResponse)
	err := grpc.Invoke(ctx, "/tau.DatasetsService/Create", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *datasetsServiceClient) CreateWithPartitions(ctx context.Context, in *CreateDatasetWithPartitionsRequest, opts ...grpc.CallOption) (*EmptyResponse, error) {
	out := new(EmptyResponse)
	err := grpc.Invoke(ctx, "/tau.DatasetsService/CreateWithPartitions", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *datasetsServiceClient) Delete(ctx context.Context, in *DeleteDatasetRequest, opts ...grpc.CallOption) (*EmptyResponse, error) {
	out := new(EmptyResponse)
	err := grpc.Invoke(ctx, "/tau.DatasetsService/Delete", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *datasetsServiceClient) ListPartitions(ctx context.Context, in *ListPartitionsRequest, opts ...grpc.CallOption) (DatasetsService_ListPartitionsClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_DatasetsService_serviceDesc.Streams[1], c.cc, "/tau.DatasetsService/ListPartitions", opts...)
	if err != nil {
		return nil, err
	}
	x := &datasetsServiceListPartitionsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DatasetsService_ListPartitionsClient interface {
	Recv() (*DatasetPartition, error)
	grpc.ClientStream
}

type datasetsServiceListPartitionsClient struct {
	grpc.ClientStream
}

func (x *datasetsServiceListPartitionsClient) Recv() (*DatasetPartition, error) {
	m := new(DatasetPartition)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for DatasetsService service

type DatasetsServiceServer interface {
	List(*EmptyRequest, DatasetsService_ListServer) error
	Get(context.Context, *GetDatasetRequest) (*Dataset, error)
	Create(context.Context, *CreateDatasetRequest) (*EmptyResponse, error)
	CreateWithPartitions(context.Context, *CreateDatasetWithPartitionsRequest) (*EmptyResponse, error)
	Delete(context.Context, *DeleteDatasetRequest) (*EmptyResponse, error)
	ListPartitions(*ListPartitionsRequest, DatasetsService_ListPartitionsServer) error
}

func RegisterDatasetsServiceServer(s *grpc.Server, srv DatasetsServiceServer) {
	s.RegisterService(&_DatasetsService_serviceDesc, srv)
}

func _DatasetsService_List_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(EmptyRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DatasetsServiceServer).List(m, &datasetsServiceListServer{stream})
}

type DatasetsService_ListServer interface {
	Send(*Dataset) error
	grpc.ServerStream
}

type datasetsServiceListServer struct {
	grpc.ServerStream
}

func (x *datasetsServiceListServer) Send(m *Dataset) error {
	return x.ServerStream.SendMsg(m)
}

func _DatasetsService_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetDatasetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DatasetsServiceServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tau.DatasetsService/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DatasetsServiceServer).Get(ctx, req.(*GetDatasetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DatasetsService_Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateDatasetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DatasetsServiceServer).Create(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tau.DatasetsService/Create",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DatasetsServiceServer).Create(ctx, req.(*CreateDatasetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DatasetsService_CreateWithPartitions_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateDatasetWithPartitionsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DatasetsServiceServer).CreateWithPartitions(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tau.DatasetsService/CreateWithPartitions",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DatasetsServiceServer).CreateWithPartitions(ctx, req.(*CreateDatasetWithPartitionsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DatasetsService_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteDatasetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DatasetsServiceServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tau.DatasetsService/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DatasetsServiceServer).Delete(ctx, req.(*DeleteDatasetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DatasetsService_ListPartitions_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListPartitionsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DatasetsServiceServer).ListPartitions(m, &datasetsServiceListPartitionsServer{stream})
}

type DatasetsService_ListPartitionsServer interface {
	Send(*DatasetPartition) error
	grpc.ServerStream
}

type datasetsServiceListPartitionsServer struct {
	grpc.ServerStream
}

func (x *datasetsServiceListPartitionsServer) Send(m *DatasetPartition) error {
	return x.ServerStream.SendMsg(m)
}

var _DatasetsService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "tau.DatasetsService",
	HandlerType: (*DatasetsServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _DatasetsService_Get_Handler,
		},
		{
			MethodName: "Create",
			Handler:    _DatasetsService_Create_Handler,
		},
		{
			MethodName: "CreateWithPartitions",
			Handler:    _DatasetsService_CreateWithPartitions_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _DatasetsService_Delete_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "List",
			Handler:       _DatasetsService_List_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ListPartitions",
			Handler:       _DatasetsService_ListPartitions_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "datasets.proto",
}

func init() { proto.RegisterFile("datasets.proto", fileDescriptor2) }

var fileDescriptor2 = []byte{
	// 845 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x55, 0x4d, 0x6f, 0xdb, 0x46,
	0x10, 0x15, 0xf5, 0x19, 0x0d, 0x65, 0x4a, 0x5e, 0x3b, 0x81, 0xaa, 0x5e, 0x1c, 0x36, 0x8d, 0xf3,
	0x01, 0x18, 0x82, 0xda, 0x22, 0x45, 0x0f, 0x05, 0x6a, 0xc5, 0x8d, 0x54, 0xd8, 0x6d, 0x40, 0xb9,
	0xe9, 0xad, 0xc4, 0x86, 0x1e, 0x56, 0x5b, 0x70, 0x49, 0x96, 0xbb, 0x54, 0x94, 0x5e, 0xfb, 0x43,
	0xfb, 0x27, 0x8a, 0xf6, 0x5a, 0xec, 0x90, 0x72, 0x24, 0x8b, 0x97, 0xdc, 0x56, 0x6f, 0xde, 0xcc,
	0xce, 0xdb, 0x19, 0x3e, 0x81, 0x73, 0xc3, 0x35, 0x57, 0xa8, 0xd5, 0x59, 0x9a, 0x25, 0x3a, 0x61,
	0x0d, 0xcd, 0xf3, 0x11, 0x04, 0x49, 0x86, 0x05, 0x30, 0x02, 0xc9, 0xf5, 0xb2, 0x38, 0xbb, 0xa7,
	0x70, 0xf8, 0x0a, 0xf5, 0xcb, 0x22, 0xc3, 0xc3, 0x3f, 0x72, 0x54, 0x9a, 0x31, 0x68, 0xc6, 0x5c,
	0xe2, 0xd0, 0x3a, 0xb1, 0x9e, 0x74, 0x3d, 0x3a, 0xbb, 0xdf, 0xc2, 0xf1, 0x34, 0x43, 0xae, 0xf1,
	0x0e, 0xf7, 0x31, 0x74, 0xca, 0xfb, 0x88, 0x6e, 0x4f, 0x7a, 0x67, 0x9a, 0xe7, 0x67, 0x1b, 0xd6,
	0x26, 0xe8, 0xfe, 0x65, 0x81, 0xbb, 0x53, 0xe0, 0x17, 0xa1, 0x97, 0xaf, 0x79, 0xa6, 0x85, 0x16,
	0x49, 0xac, 0x3e, 0xb2, 0x1c, 0xfb, 0x0a, 0x20, 0xbd, 0x4d, 0x1e, 0xd6, 0x4f, 0x1a, 0x4f, 0xec,
	0xc9, 0xfd, 0x6d, 0xea, 0x6d, 0x69, 0x6f, 0x8b, 0xe8, 0x3e, 0x83, 0xe3, 0x97, 0x18, 0xe1, 0x9e,
	0x8a, 0x2a, 0xc5, 0xdf, 0xc0, 0xfd, 0x4b, 0xa1, 0xf4, 0x7e, 0x8f, 0x0f, 0xa1, 0x57, 0xb6, 0xe1,
	0x6f, 0x25, 0xd9, 0x25, 0xf6, 0xa3, 0xc9, 0x7d, 0x03, 0x87, 0xe7, 0x3a, 0x43, 0x9c, 0xc7, 0x37,
	0xb8, 0xfe, 0x29, 0xa5, 0x74, 0xf6, 0x29, 0x74, 0xe3, 0x5c, 0xfa, 0x06, 0x57, 0x94, 0xd4, 0xf2,
	0xee, 0xc5, 0xb9, 0xbc, 0x36, 0xbf, 0xd9, 0x23, 0x70, 0x24, 0x5f, 0xfb, 0x11, 0xf2, 0xd0, 0x17,
	0x1a, 0xa5, 0x11, 0x65, 0x18, 0x3d, 0xc9, 0xd7, 0x97, 0xc8, 0xc3, 0xb9, 0xc1, 0xdc, 0x5f, 0xe1,
	0xe8, 0x4d, 0x92, 0x25, 0x71, 0x22, 0x76, 0x2a, 0x3f, 0x84, 0x9e, 0x4a, 0x23, 0xa1, 0xfd, 0x90,
	0x07, 0x3a, 0xc9, 0xca, 0xe2, 0x36, 0x61, 0xdf, 0x13, 0xb4, 0xa9, 0x1f, 0x60, 0x14, 0xed, 0xd5,
	0x9f, 0x62, 0x14, 0x15, 0xf5, 0xff, 0xb5, 0x60, 0x30, 0x8b, 0xd5, 0xbb, 0x9d, 0xea, 0x53, 0x18,
	0x28, 0xe4, 0x59, 0xb0, 0xf4, 0x79, 0xf4, 0x5b, 0x92, 0x09, 0xbd, 0x94, 0x74, 0x83, 0x33, 0x19,
	0xd2, 0x8b, 0x9b, 0x84, 0x05, 0x11, 0xbe, 0xdb, 0xc4, 0xbd, 0xbe, 0xda, 0x05, 0xd8, 0x53, 0x18,
	0x44, 0xb8, 0xc2, 0xc8, 0x97, 0x79, 0xa4, 0x45, 0x1a, 0x09, 0xcc, 0xa8, 0x83, 0xba, 0xd7, 0x27,
	0xfc, 0xea, 0x16, 0x66, 0x0e, 0xd4, 0x31, 0x1c, 0x36, 0xa8, 0xbd, 0x3a, 0x86, 0xec, 0x14, 0xfa,
	0x18, 0xfa, 0x41, 0x12, 0x2b, 0x9d, 0xe5, 0x81, 0xe9, 0x69, 0xd8, 0xa4, 0xa0, 0x83, 0xe1, 0x74,
	0x0b, 0x65, 0x3d, 0xb0, 0xe4, 0xb0, 0x45, 0x21, 0x4b, 0xb2, 0x23, 0x68, 0x49, 0x5f, 0xf2, 0xf5,
	0xb0, 0x4d, 0x48, 0x53, 0x5e, 0xf1, 0x35, 0x7b, 0x00, 0x1d, 0x02, 0xfd, 0xf1, 0xb0, 0x43, 0x70,
	0xcb, 0xc0, 0x63, 0xf7, 0x6f, 0x0b, 0x5a, 0x24, 0xda, 0xac, 0x82, 0x12, 0x7f, 0x62, 0xf9, 0x86,
	0x74, 0x66, 0x8f, 0xa0, 0xa5, 0x52, 0x1e, 0x20, 0x75, 0xec, 0x4c, 0x1c, 0x92, 0xbd, 0x30, 0xc8,
	0xf5, 0xfb, 0x14, 0xbd, 0x22, 0xc8, 0xce, 0xa0, 0xf5, 0xd6, 0x0c, 0x97, 0x5a, 0xb7, 0x27, 0x0f,
	0x88, 0xb5, 0xb7, 0x06, 0xb3, 0x9a, 0x57, 0xd0, 0xd8, 0x97, 0xd0, 0x59, 0x15, 0xc3, 0x24, 0x3d,
	0x76, 0xf9, 0x9c, 0x15, 0x03, 0x9e, 0xd5, 0xbc, 0x0d, 0x95, 0x3d, 0x87, 0xe6, 0x32, 0x56, 0xef,
	0x48, 0xe7, 0x66, 0xe7, 0xef, 0x8e, 0x6c, 0x56, 0xf3, 0x88, 0x74, 0xde, 0x85, 0x4e, 0x52, 0x40,
	0x6e, 0x0f, 0x60, 0xaa, 0x56, 0xe5, 0xde, 0xbb, 0x07, 0x60, 0xff, 0xa0, 0x92, 0x78, 0xf3, 0xf3,
	0x1f, 0x0b, 0x3a, 0xe5, 0xb9, 0xea, 0x5b, 0x30, 0x58, 0xca, 0xf5, 0x92, 0xf4, 0x77, 0x3d, 0x3a,
	0xb3, 0xcf, 0xc1, 0x31, 0xeb, 0xbc, 0xf5, 0x19, 0x16, 0x23, 0x3b, 0x88, 0x73, 0xf9, 0xe1, 0xa3,
	0x31, 0xbb, 0x69, 0x68, 0x19, 0xa6, 0x91, 0x08, 0xb8, 0x2a, 0x47, 0x67, 0xc7, 0xb9, 0xf4, 0x4a,
	0x88, 0x9d, 0x40, 0x4b, 0x98, 0xee, 0x4b, 0x4d, 0x40, 0x9a, 0x48, 0x8f, 0x57, 0x04, 0xd8, 0x67,
	0xd0, 0x08, 0xd4, 0x8a, 0x26, 0x69, 0x4f, 0xfa, 0x14, 0xff, 0x20, 0x66, 0x56, 0xf3, 0x4c, 0x94,
	0x3d, 0x86, 0xe6, 0xef, 0x2a, 0x89, 0x69, 0xb0, 0xf6, 0x64, 0x40, 0xac, 0x2d, 0x91, 0xe6, 0x51,
	0x4c, 0xfc, 0xfc, 0x1e, 0xb4, 0xc3, 0x24, 0x93, 0x5c, 0xbb, 0x5f, 0xc3, 0xe0, 0xae, 0x5d, 0x98,
	0xed, 0x13, 0x37, 0xe5, 0xf4, 0xeb, 0xe2, 0x86, 0x1d, 0x43, 0x2b, 0x14, 0x11, 0x16, 0x26, 0xd3,
	0xf5, 0x8a, 0x1f, 0xcf, 0xc6, 0x70, 0x54, 0xb1, 0xf6, 0x0c, 0xa0, 0xbd, 0x98, 0x5f, 0xbd, 0xbe,
	0xbc, 0x18, 0xd4, 0xd8, 0x01, 0x74, 0x67, 0x17, 0x3f, 0x7b, 0xf3, 0xc5, 0xf5, 0x7c, 0x3a, 0xb0,
	0x26, 0xff, 0xd5, 0xa1, 0x5f, 0x5e, 0xa6, 0x16, 0x98, 0xad, 0x44, 0x80, 0xec, 0x29, 0x34, 0x8d,
	0xc5, 0xb0, 0x43, 0xea, 0xf5, 0x42, 0xa6, 0xfa, 0x7d, 0x69, 0x32, 0xa3, 0x1d, 0xdf, 0x1b, 0x5b,
	0xec, 0x39, 0x34, 0x5e, 0xa1, 0x66, 0xc5, 0x52, 0xed, 0x59, 0xf6, 0x2e, 0x9d, 0xbd, 0x80, 0x76,
	0xe1, 0xb5, 0xec, 0x93, 0xe2, 0xad, 0x2a, 0x9c, 0x7b, 0xc4, 0xb6, 0x2f, 0x55, 0x69, 0x12, 0x2b,
	0x64, 0x8b, 0x8d, 0xcb, 0xef, 0xba, 0x33, 0x3b, 0xdd, 0x2f, 0x53, 0xe9, 0xdf, 0x95, 0x45, 0x5f,
	0x40, 0xbb, 0x30, 0xdd, 0xb2, 0x9b, 0x2a, 0x07, 0xae, 0x4c, 0xbc, 0x00, 0x67, 0xd7, 0x81, 0xd9,
	0x88, 0x58, 0x95, 0xb6, 0x3c, 0xaa, 0xb6, 0xff, 0xb1, 0xf5, 0xb6, 0x4d, 0x7f, 0x75, 0x5f, 0xfc,
	0x1f, 0x00, 0x00, 0xff, 0xff, 0x18, 0xac, 0x8e, 0x9f, 0x19, 0x07, 0x00, 0x00,
}
