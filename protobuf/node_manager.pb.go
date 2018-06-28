// Code generated by protoc-gen-go. DO NOT EDIT.
// source: node_manager.proto

package tau

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type Node struct {
	Address string `protobuf:"bytes,1,opt,name=address" json:"address,omitempty"`
}

func (m *Node) Reset()                    { *m = Node{} }
func (m *Node) String() string            { return proto.CompactTextString(m) }
func (*Node) ProtoMessage()               {}
func (*Node) Descriptor() ([]byte, []int) { return fileDescriptor2, []int{0} }

func (m *Node) GetAddress() string {
	if m != nil {
		return m.Address
	}
	return ""
}

func init() {
	proto.RegisterType((*Node)(nil), "tau.Node")
}

func init() { proto.RegisterFile("node_manager.proto", fileDescriptor2) }

var fileDescriptor2 = []byte{
	// 82 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0xca, 0xcb, 0x4f, 0x49,
	0x8d, 0xcf, 0x4d, 0xcc, 0x4b, 0x4c, 0x4f, 0x2d, 0xd2, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62,
	0x2e, 0x49, 0x2c, 0x55, 0x52, 0xe0, 0x62, 0xf1, 0xcb, 0x4f, 0x49, 0x15, 0x92, 0xe0, 0x62, 0x4f,
	0x4c, 0x49, 0x29, 0x4a, 0x2d, 0x2e, 0x96, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0c, 0x82, 0x71, 0x93,
	0xd8, 0xc0, 0xaa, 0x8d, 0x01, 0x01, 0x00, 0x00, 0xff, 0xff, 0x93, 0x85, 0x51, 0x83, 0x43, 0x00,
	0x00, 0x00,
}
