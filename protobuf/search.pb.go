// Code generated by protoc-gen-go. DO NOT EDIT.
// source: search.proto

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

type SearchRequest struct {
	DatasetName string    `protobuf:"bytes,1,opt,name=dataset_name,json=datasetName" json:"dataset_name,omitempty"`
	K           int32     `protobuf:"varint,2,opt,name=k" json:"k,omitempty"`
	Query       []float32 `protobuf:"fixed32,3,rep,packed,name=query" json:"query,omitempty"`
}

func (m *SearchRequest) Reset()                    { *m = SearchRequest{} }
func (m *SearchRequest) String() string            { return proto.CompactTextString(m) }
func (*SearchRequest) ProtoMessage()               {}
func (*SearchRequest) Descriptor() ([]byte, []int) { return fileDescriptor4, []int{0} }

func (m *SearchRequest) GetDatasetName() string {
	if m != nil {
		return m.DatasetName
	}
	return ""
}

func (m *SearchRequest) GetK() int32 {
	if m != nil {
		return m.K
	}
	return 0
}

func (m *SearchRequest) GetQuery() []float32 {
	if m != nil {
		return m.Query
	}
	return nil
}

type SearchPartitionsRequest struct {
	DatasetName string    `protobuf:"bytes,1,opt,name=dataset_name,json=datasetName" json:"dataset_name,omitempty"`
	K           int32     `protobuf:"varint,2,opt,name=k" json:"k,omitempty"`
	Query       []float32 `protobuf:"fixed32,3,rep,packed,name=query" json:"query,omitempty"`
	Partitions  []string  `protobuf:"bytes,4,rep,name=partitions" json:"partitions,omitempty"`
}

func (m *SearchPartitionsRequest) Reset()                    { *m = SearchPartitionsRequest{} }
func (m *SearchPartitionsRequest) String() string            { return proto.CompactTextString(m) }
func (*SearchPartitionsRequest) ProtoMessage()               {}
func (*SearchPartitionsRequest) Descriptor() ([]byte, []int) { return fileDescriptor4, []int{1} }

func (m *SearchPartitionsRequest) GetDatasetName() string {
	if m != nil {
		return m.DatasetName
	}
	return ""
}

func (m *SearchPartitionsRequest) GetK() int32 {
	if m != nil {
		return m.K
	}
	return 0
}

func (m *SearchPartitionsRequest) GetQuery() []float32 {
	if m != nil {
		return m.Query
	}
	return nil
}

func (m *SearchPartitionsRequest) GetPartitions() []string {
	if m != nil {
		return m.Partitions
	}
	return nil
}

type SearchResultItem struct {
	Id       int64   `protobuf:"varint,1,opt,name=id" json:"id,omitempty"`
	Distance float32 `protobuf:"fixed32,2,opt,name=distance" json:"distance,omitempty"`
}

func (m *SearchResultItem) Reset()                    { *m = SearchResultItem{} }
func (m *SearchResultItem) String() string            { return proto.CompactTextString(m) }
func (*SearchResultItem) ProtoMessage()               {}
func (*SearchResultItem) Descriptor() ([]byte, []int) { return fileDescriptor4, []int{2} }

func (m *SearchResultItem) GetId() int64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *SearchResultItem) GetDistance() float32 {
	if m != nil {
		return m.Distance
	}
	return 0
}

func init() {
	proto.RegisterType((*SearchRequest)(nil), "tau.SearchRequest")
	proto.RegisterType((*SearchPartitionsRequest)(nil), "tau.SearchPartitionsRequest")
	proto.RegisterType((*SearchResultItem)(nil), "tau.SearchResultItem")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for SearchService service

type SearchServiceClient interface {
	Search(ctx context.Context, in *SearchRequest, opts ...grpc.CallOption) (SearchService_SearchClient, error)
	SearchPartitions(ctx context.Context, in *SearchPartitionsRequest, opts ...grpc.CallOption) (SearchService_SearchPartitionsClient, error)
}

type searchServiceClient struct {
	cc *grpc.ClientConn
}

func NewSearchServiceClient(cc *grpc.ClientConn) SearchServiceClient {
	return &searchServiceClient{cc}
}

func (c *searchServiceClient) Search(ctx context.Context, in *SearchRequest, opts ...grpc.CallOption) (SearchService_SearchClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_SearchService_serviceDesc.Streams[0], c.cc, "/tau.SearchService/Search", opts...)
	if err != nil {
		return nil, err
	}
	x := &searchServiceSearchClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type SearchService_SearchClient interface {
	Recv() (*SearchResultItem, error)
	grpc.ClientStream
}

type searchServiceSearchClient struct {
	grpc.ClientStream
}

func (x *searchServiceSearchClient) Recv() (*SearchResultItem, error) {
	m := new(SearchResultItem)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *searchServiceClient) SearchPartitions(ctx context.Context, in *SearchPartitionsRequest, opts ...grpc.CallOption) (SearchService_SearchPartitionsClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_SearchService_serviceDesc.Streams[1], c.cc, "/tau.SearchService/SearchPartitions", opts...)
	if err != nil {
		return nil, err
	}
	x := &searchServiceSearchPartitionsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type SearchService_SearchPartitionsClient interface {
	Recv() (*SearchResultItem, error)
	grpc.ClientStream
}

type searchServiceSearchPartitionsClient struct {
	grpc.ClientStream
}

func (x *searchServiceSearchPartitionsClient) Recv() (*SearchResultItem, error) {
	m := new(SearchResultItem)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for SearchService service

type SearchServiceServer interface {
	Search(*SearchRequest, SearchService_SearchServer) error
	SearchPartitions(*SearchPartitionsRequest, SearchService_SearchPartitionsServer) error
}

func RegisterSearchServiceServer(s *grpc.Server, srv SearchServiceServer) {
	s.RegisterService(&_SearchService_serviceDesc, srv)
}

func _SearchService_Search_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SearchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(SearchServiceServer).Search(m, &searchServiceSearchServer{stream})
}

type SearchService_SearchServer interface {
	Send(*SearchResultItem) error
	grpc.ServerStream
}

type searchServiceSearchServer struct {
	grpc.ServerStream
}

func (x *searchServiceSearchServer) Send(m *SearchResultItem) error {
	return x.ServerStream.SendMsg(m)
}

func _SearchService_SearchPartitions_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SearchPartitionsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(SearchServiceServer).SearchPartitions(m, &searchServiceSearchPartitionsServer{stream})
}

type SearchService_SearchPartitionsServer interface {
	Send(*SearchResultItem) error
	grpc.ServerStream
}

type searchServiceSearchPartitionsServer struct {
	grpc.ServerStream
}

func (x *searchServiceSearchPartitionsServer) Send(m *SearchResultItem) error {
	return x.ServerStream.SendMsg(m)
}

var _SearchService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "tau.SearchService",
	HandlerType: (*SearchServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Search",
			Handler:       _SearchService_Search_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SearchPartitions",
			Handler:       _SearchService_SearchPartitions_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "search.proto",
}

func init() { proto.RegisterFile("search.proto", fileDescriptor4) }

var fileDescriptor4 = []byte{
	// 250 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x91, 0xc1, 0x4b, 0xc3, 0x30,
	0x14, 0xc6, 0x49, 0xe2, 0x86, 0x7b, 0x4e, 0x91, 0x87, 0x62, 0x19, 0x22, 0xb5, 0xa7, 0x9e, 0x8a,
	0x28, 0x5e, 0xbd, 0xef, 0x22, 0x92, 0x81, 0x57, 0x79, 0xb6, 0x0f, 0x0c, 0xb3, 0xed, 0x96, 0xbc,
	0x08, 0xde, 0xfc, 0x17, 0xfc, 0x8f, 0x85, 0xd6, 0xce, 0x0d, 0xf1, 0xe6, 0xf1, 0xfb, 0x11, 0xbe,
	0x2f, 0xf9, 0x05, 0xa6, 0x81, 0xc9, 0x97, 0x2f, 0xc5, 0xca, 0xb7, 0xd2, 0xa2, 0x11, 0x8a, 0xd9,
	0x23, 0x1c, 0x2e, 0x3a, 0x68, 0x79, 0x1d, 0x39, 0x08, 0x5e, 0xc2, 0xb4, 0x22, 0xa1, 0xc0, 0xf2,
	0xd4, 0x50, 0xcd, 0x89, 0x4a, 0x55, 0x3e, 0xb1, 0x07, 0xdf, 0xec, 0x9e, 0x6a, 0xc6, 0x29, 0xa8,
	0x65, 0xa2, 0x53, 0x95, 0x8f, 0xac, 0x5a, 0xe2, 0x09, 0x8c, 0xd6, 0x91, 0xfd, 0x7b, 0x62, 0x52,
	0x93, 0x6b, 0xdb, 0x87, 0xec, 0x43, 0xc1, 0x59, 0x5f, 0xfc, 0x40, 0x5e, 0x9c, 0xb8, 0xb6, 0x09,
	0xff, 0x3b, 0x81, 0x17, 0x00, 0xab, 0x4d, 0x77, 0xb2, 0x97, 0x9a, 0x7c, 0x62, 0xb7, 0x48, 0x76,
	0x07, 0xc7, 0xc3, 0xd3, 0x42, 0x7c, 0x95, 0xb9, 0x70, 0x8d, 0x47, 0xa0, 0x5d, 0xd5, 0x0d, 0x1a,
	0xab, 0x5d, 0x85, 0x33, 0xd8, 0xaf, 0x5c, 0x10, 0x6a, 0x4a, 0xee, 0xe6, 0xb4, 0xdd, 0xe4, 0xeb,
	0x4f, 0x35, 0xb8, 0x59, 0xb0, 0x7f, 0x73, 0x25, 0xe3, 0x2d, 0x8c, 0x7b, 0x80, 0x58, 0x08, 0xc5,
	0x62, 0xc7, 0xdc, 0xec, 0x74, 0x87, 0x0d, 0x93, 0x57, 0x0a, 0xe7, 0xc3, 0x45, 0x7e, 0x54, 0xe0,
	0xf9, 0xd6, 0xe1, 0x5f, 0x86, 0xfe, 0xac, 0x7a, 0x1e, 0x77, 0x5f, 0x77, 0xf3, 0x15, 0x00, 0x00,
	0xff, 0xff, 0x6a, 0x17, 0xb3, 0xfb, 0xca, 0x01, 0x00, 0x00,
}
