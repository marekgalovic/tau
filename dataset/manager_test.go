package dataset

import (
    "fmt";
    "testing";

    "github.com/marekgalovic/tau/cluster";
    "github.com/marekgalovic/tau/storage";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";

    "github.com/stretchr/testify/suite";
    "github.com/golang/mock/gomock";
)

type ManagerTestSuite struct {
    suite.Suite
    mockController *gomock.Controller
    zk *utils.MockZookeeper
    cluster *cluster.MockCluster
    storage *storage.MockStorage
    manager *manager
}

func (suite *ManagerTestSuite) SetupTest() {
    suite.mockController = gomock.NewController(suite.T())
    suite.zk = utils.NewMockZookeeper(suite.mockController)
    suite.cluster = cluster.NewMockCluster(suite.mockController)
    suite.storage = storage.NewMockStorage(suite.mockController)

    suite.zk.EXPECT().CreatePath("datasets", nil, int32(0))

    var err error
    suite.manager, err = NewManager(DatasetManagerConfig{IndicesPath: "/indices"}, suite.zk, suite.cluster, suite.storage)
    if err != nil {
        suite.Fail(fmt.Sprintf("%s", err))
    }
}

func (suite *ManagerTestSuite) TearDownTest() {
    suite.mockController.Finish()
}

func (suite *ManagerTestSuite) TestDatasetsCacheMethods() {
    d, exists := suite.manager.GetDataset("foo")
    suite.Nil(d)
    suite.False(exists)

    suite.manager.addDataset(&dataset{meta: &pb.Dataset{Name: "foo"}})

    d, exists = suite.manager.GetDataset("foo")
    suite.NotNil(d)
    suite.True(exists)

    suite.manager.deleteDataset("foo")

    d, exists = suite.manager.GetDataset("foo")
    suite.Nil(d)
    suite.False(exists)
}

func (suite *ManagerTestSuite) TestNodeCreated() {
    d := NewMockDataset(suite.mockController)
    dPb := &pb.Dataset{Name: "foo", NumReplicas: 1}
    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().LocalPartitions().Return([]interface{}{"1"})
    suite.manager.addDataset(d)
    suite.manager.localDatasets.Add("foo")

    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().Meta().Return(dPb)
    suite.cluster.EXPECT().Uuid().Return("node1")
    suite.cluster.EXPECT().GetTopHrwNodes(1, "foo.1").Return(utils.NewSet("node1"), nil)

    suite.manager.nodeCreated(cluster.NewNode(&pb.Node{Uuid: "node2"}, suite.cluster))
}

func (suite *ManagerTestSuite) TestNodeCreatedReleasesPartitions() {
    d := NewMockDataset(suite.mockController)
    dPb := &pb.Dataset{Name: "foo", NumReplicas: 1}
    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().LocalPartitions().Return([]interface{}{"1"})
    suite.manager.addDataset(d)
    suite.manager.localDatasets.Add("foo")

    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().Meta().Return(dPb)
    suite.cluster.EXPECT().Uuid().Return("node1")
    suite.cluster.EXPECT().GetTopHrwNodes(1, "foo.1").Return(utils.NewSet("node2"), nil)

    d.EXPECT().DeletePartitions(utils.NewSet("1"))

    suite.manager.nodeCreated(cluster.NewNode(&pb.Node{Uuid: "node2"}, suite.cluster))
}

func (suite *ManagerTestSuite) TestNodeDeleted() {
    d := NewMockDataset(suite.mockController)
    dPb := &pb.Dataset{Name: "foo", NumReplicas: 1}
    d.EXPECT().Meta().Return(dPb)
    suite.manager.addDataset(d)

    suite.zk.EXPECT().Children("datasets").Return([]string{"foo"}, nil)
    suite.zk.EXPECT().Children("datasets/foo/partitions").Return([]string{"1"}, nil)
    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().Meta().Return(dPb)
    suite.cluster.EXPECT().Uuid().Return("node1")
    suite.cluster.EXPECT().GetTopHrwNodes(1, "foo.1").Return(utils.NewSet("node3"), nil)

    suite.manager.nodeDeleted(cluster.NewNode(&pb.Node{Uuid: "node2"}, suite.cluster))
}

func (suite *ManagerTestSuite) TestNodeDeletedAcquiresPartitionOwnership() {
    d := NewMockDataset(suite.mockController)
    dPb := &pb.Dataset{Name: "foo", NumReplicas: 1}
    d.EXPECT().Meta().Return(dPb)
    suite.manager.addDataset(d)

    suite.zk.EXPECT().Children("datasets").Return([]string{"foo"}, nil)
    suite.zk.EXPECT().Children("datasets/foo/partitions").Return([]string{"1"}, nil)
    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().Meta().Return(dPb)
    suite.cluster.EXPECT().Uuid().Return("node1")
    suite.cluster.EXPECT().GetTopHrwNodes(1, "foo.1").Return(utils.NewSet("node1"), nil)

    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().BuildPartitions(utils.NewSet("1"))

    suite.manager.nodeDeleted(cluster.NewNode(&pb.Node{Uuid: "node2"}, suite.cluster))
}

func (suite *ManagerTestSuite) TestDatasetCreated() {
    d := NewMockDataset(suite.mockController)
    dPb := &pb.Dataset{Name: "foo", NumReplicas: 1}
    d.EXPECT().Meta().Return(dPb)

    suite.zk.EXPECT().Children("datasets/foo/partitions").Return([]string{"1", "2"}, nil)

    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().Meta().Return(dPb)
    suite.cluster.EXPECT().Uuid().Return("node1")
    suite.cluster.EXPECT().GetTopHrwNodes(1, "foo.1").Return(utils.NewSet("node2"), nil)

    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().Meta().Return(dPb)
    suite.cluster.EXPECT().Uuid().Return("node1")
    suite.cluster.EXPECT().GetTopHrwNodes(1, "foo.2").Return(utils.NewSet("node1"), nil)

    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().BuildPartitions(utils.NewSet("2"))

    suite.manager.datasetCreated(d)
}

func (suite *ManagerTestSuite) TestDatasetDeleted() {
    suite.manager.localDatasets.Add("foo")

    d := NewMockDataset(suite.mockController)
    dPb := &pb.Dataset{Name: "foo", NumReplicas: 1}
    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().Meta().Return(dPb)
    d.EXPECT().DeleteAllPartitions()
    d.EXPECT().Meta().Return(dPb)

    suite.manager.datasetDeleted(d)
}

func (suite *ManagerTestSuite) TestDatasetDeletedWithNoLocalPartitions() {

    d := NewMockDataset(suite.mockController)
    dPb := &pb.Dataset{Name: "foo", NumReplicas: 1}
    d.EXPECT().Meta().Return(dPb)

    suite.manager.datasetDeleted(d)
}

func TestManagerTestSuite(t *testing.T) {
    suite.Run(t, new(ManagerTestSuite))
}
