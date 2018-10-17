package tau

import (
    "net";

    "github.com/marekgalovic/tau/pkg/storage";
    "github.com/marekgalovic/tau/pkg/dataset";
    pb "github.com/marekgalovic/tau/pkg/protobuf";
    "github.com/marekgalovic/tau/pkg/utils";

    "google.golang.org/grpc"
)

type Server interface {
    Start() error
    Stop()
}

type server struct {
    listener net.Listener
    grpcServer *grpc.Server
    
    config *Config
    zk utils.Zookeeper
    datasetsManager dataset.Manager
    storage storage.Storage
}

func NewServer(config *Config, zk utils.Zookeeper, datasetsManager dataset.Manager, storage storage.Storage) Server {
    s := &server{
        config: config,
        zk: zk,
        datasetsManager: datasetsManager,
        storage: storage,
    }

    s.initializeGrpcServer()
    s.registerServices()

    return s
}

func (s *server) Start() error {
    var err error

    s.listener, err = net.Listen("tcp", s.config.BindAddress())
    if err != nil {
        return err
    }

    go s.grpcServer.Serve(s.listener)
    return nil
}

func (s *server) Stop() {
    s.grpcServer.GracefulStop()
    s.listener.Close()
}

func (s *server) initializeGrpcServer() {
    s.grpcServer = grpc.NewServer()
}

func (s *server) registerServices() {
    pb.RegisterDatasetsServiceServer(s.grpcServer, newDatasetsService(s.config, s.zk, s.storage))
    pb.RegisterSearchServiceServer(s.grpcServer, newSearchService(s.datasetsManager))
}
