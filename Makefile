compile_protos:
	protoc -I ./protobuf/proto --go_out=plugins=grpc:./protobuf ./protobuf/proto/*.proto
