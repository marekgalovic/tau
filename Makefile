install:
	go get -t -d -v ./...

build:
	GOOS=linux GOARCH=amd64 go build -o ./bin/tau_server-linux-amd64 ./cmd/server/main.go
	GOOS=darwin GOARCH=amd64 go build -o ./bin/tau_server-darwin-amd64 ./cmd/server/main.go

compile_protos:
	protoc -I ./protobuf/proto --go_out=plugins=grpc:./protobuf ./protobuf/proto/*.proto

generate_mocks:
	mockgen --source utils/zookeeper.go --destination utils/zookeeper_mock.go --package utils
	mockgen --source cluster/cluster.go --destination cluster/cluster_mock.go --package cluster
	mockgen --source storage/storage.go --destination storage/storage_mock.go --package storage
	mockgen --source dataset/dataset.go --destination dataset/dataset_mock.go --package dataset
