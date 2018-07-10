package tau

import (
    "net";

    pb "github.com/marekgalovic/tau/protobuf";

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
    datasetsManager DatasetsManager
}

func NewServer(config *Config, datasetsManager DatasetsManager) Server {
    s := &server{
        config: config,
        datasetsManager: datasetsManager,
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
    pb.RegisterSearchServiceServer(s.grpcServer, newSearchService(s.datasetsManager))
}
