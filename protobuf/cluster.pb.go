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
	ZkPath    string `protobuf:"bytes,1,opt,name=zk_path,json=zkPath" json:"zk_path,omitempty"`
	SeqId     int64  `protobuf:"varint,2,opt,name=seqId" json:"seqId,omitempty"`
	Uuid      string `protobuf:"bytes,3,opt,name=uuid" json:"uuid,omitempty"`
	IpAddress string `protobuf:"bytes,4,opt,name=ip_address,json=ipAddress" json:"ip_address,omitempty"`
	Port      string `protobuf:"bytes,5,opt,name=port" json:"port,omitempty"`
}

func (m *Node) Reset()                    { *m = Node{} }
func (m *Node) String() string            { return proto.CompactTextString(m) }
func (*Node) ProtoMessage()               {}
func (*Node) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Node) GetZkPath() string {
	if m != nil {
		return m.ZkPath
	}
	return ""
}

func (m *Node) GetSeqId() int64 {
	if m != nil {
		return m.SeqId
	}
	return 0
}

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
	// 149 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x4d, 0xce, 0x29, 0x2d,
	0x2e, 0x49, 0x2d, 0xd2, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2e, 0x49, 0x2c, 0x55, 0xaa,
	0xe1, 0x62, 0xf1, 0xcb, 0x4f, 0x49, 0x15, 0x12, 0xe7, 0x62, 0xaf, 0xca, 0x8e, 0x2f, 0x48, 0x2c,
	0xc9, 0x90, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0c, 0x62, 0xab, 0xca, 0x0e, 0x48, 0x2c, 0xc9, 0x10,
	0x12, 0xe1, 0x62, 0x2d, 0x4e, 0x2d, 0xf4, 0x4c, 0x91, 0x60, 0x52, 0x60, 0xd4, 0x60, 0x0e, 0x82,
	0x70, 0x84, 0x84, 0xb8, 0x58, 0x4a, 0x4b, 0x33, 0x53, 0x24, 0x98, 0xc1, 0x6a, 0xc1, 0x6c, 0x21,
	0x59, 0x2e, 0xae, 0xcc, 0x82, 0xf8, 0xc4, 0x94, 0x94, 0xa2, 0xd4, 0xe2, 0x62, 0x09, 0x16, 0xb0,
	0x0c, 0x67, 0x66, 0x81, 0x23, 0x44, 0x00, 0xa4, 0xa5, 0x20, 0xbf, 0xa8, 0x44, 0x82, 0x15, 0xa2,
	0x05, 0xc4, 0x4e, 0x62, 0x03, 0xbb, 0xc4, 0x18, 0x10, 0x00, 0x00, 0xff, 0xff, 0x52, 0xea, 0xa8,
	0xfb, 0x9a, 0x00, 0x00, 0x00,
}
