// Code generated by protoc-gen-go. DO NOT EDIT.
// source: cluster.proto

/*
Package tau is a generated protocol buffer package.

It is generated from these files:
	cluster.proto
	core.proto
	datasets.proto
	search.proto

It has these top-level messages:
	Node
	EmptyRequest
	EmptyResponse
	GetDatasetRequest
	CreateDatasetRequest
	CreateDatasetWithPartitionsRequest
	DeleteDatasetRequest
	ListPartitionsRequest
	BtreeIndexOptions
	VoronoiIndexOptions
	Index
	CsvDataset
	JsonDataset
	Dataset
	DatasetPartition
	SearchRequest
	SearchPartitionsRequest
	SearchResultItem
*/
package tau

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Node struct {
	Uuid      string `protobuf:"bytes,1,opt,name=uuid" json:"uuid,omitempty"`
	IpAddress string `protobuf:"bytes,2,opt,name=ip_address,json=ipAddress" json:"ip_address,omitempty"`
	Port      string `protobuf:"bytes,3,opt,name=port" json:"port,omitempty"`
}

func (m *Node) Reset()                    { *m = Node{} }
func (m *Node) String() string            { return proto.CompactTextString(m) }
func (*Node) ProtoMessage()               {}
func (*Node) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Node) GetUuid() string {
	if m != nil {
		return m.Uuid
	}
	return ""
}

func (m *Node) GetIpAddress() string {
	if m != nil {
		return m.IpAddress
	}
	return ""
}

func (m *Node) GetPort() string {
	if m != nil {
		return m.Port
	}
	return ""
}

func init() {
	proto.RegisterType((*Node)(nil), "tau.Node")
}

func init() { proto.RegisterFile("cluster.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 109 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x4d, 0xce, 0x29, 0x2d,
	0x2e, 0x49, 0x2d, 0xd2, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2e, 0x49, 0x2c, 0x55, 0xf2,
	0xe5, 0x62, 0xf1, 0xcb, 0x4f, 0x49, 0x15, 0x12, 0xe2, 0x62, 0x29, 0x2d, 0xcd, 0x4c, 0x91, 0x60,
	0x54, 0x60, 0xd4, 0xe0, 0x0c, 0x02, 0xb3, 0x85, 0x64, 0xb9, 0xb8, 0x32, 0x0b, 0xe2, 0x13, 0x53,
	0x52, 0x8a, 0x52, 0x8b, 0x8b, 0x25, 0x98, 0xc0, 0x32, 0x9c, 0x99, 0x05, 0x8e, 0x10, 0x01, 0x90,
	0x96, 0x82, 0xfc, 0xa2, 0x12, 0x09, 0x66, 0x88, 0x16, 0x10, 0x3b, 0x89, 0x0d, 0x6c, 0xb4, 0x31,
	0x20, 0x00, 0x00, 0xff, 0xff, 0xa4, 0x7f, 0x1c, 0x8b, 0x6b, 0x00, 0x00, 0x00,
}
