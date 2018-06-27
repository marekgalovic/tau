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
    
    datasetsManager DatasetsManager
}

func NewServer(datasetsManager DatasetsManager) Server {
    s := &server{
        datasetsManager: datasetsManager,
    }

    s.initializeGrpcServer()
    s.registerServices()

    return s
}

func (s *server) Start() error {
    var err error

    s.listener, err = net.Listen("tcp", s.bindAddress())
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

func (s *server) bindAddress() string {
    return ":5555"
}

func (s *server) initializeGrpcServer() {
    s.grpcServer = grpc.NewServer()
}

func (s *server) registerServices() {
    // pb.RegisterDatasetsServiceServer(s.grpcServer, newDatasetsService(s.datasetsManager))
    pb.RegisterSearchServiceServer(s.grpcServer, newSearchService(s.datasetsManager))
}
