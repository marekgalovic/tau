package tau

import (
    "net";

    pb "github.com/marekgalovic/tau/protobuf";

    "github.com/samuel/go-zookeeper/zk";
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
    zk *zk.Conn
    datasetsManager DatasetsManager
}

func NewServer(config *Config, zkConn *zk.Conn, datasetsManager DatasetsManager) Server {
    s := &server{
        config: config,
        zk: zkConn,
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
    pb.RegisterDatasetsServiceServer(s.grpcServer, newDatasetsService(s.config, s.zk))
    pb.RegisterSearchServiceServer(s.grpcServer, newSearchService(s.datasetsManager))
}
