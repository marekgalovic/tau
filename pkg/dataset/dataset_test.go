package dataset

import (
    // "fmt";
    "context";
    "testing";

    "github.com/marekgalovic/tau/pkg/cluster";
    "github.com/marekgalovic/tau/pkg/storage";
    pb "github.com/marekgalovic/tau/pkg/protobuf";
    "github.com/marekgalovic/tau/pkg/utils";

    // "github.com/golang/protobuf/proto";
    "github.com/stretchr/testify/suite";
    "github.com/golang/mock/gomock";
)

type DatasetTestSuite struct {
    suite.Suite
    mockController *gomock.Controller
    zk *utils.MockZookeeper
    cluster *cluster.MockCluster
    storage *storage.MockStorage
    dataset *dataset
}

func (suite *DatasetTestSuite) SetupTest() {
    suite.mockController = gomock.NewController(suite.T())
    suite.zk = utils.NewMockZookeeper(suite.mockController)
    suite.cluster = cluster.NewMockCluster(suite.mockController)
    suite.storage = storage.NewMockStorage(suite.mockController)

    suite.dataset = &dataset {
        ctx: context.Background(),
        config: DatasetManagerConfig {IndicesPath: "/indices"},
        meta: &pb.Dataset{
            Name: "foo",
            NumReplicas: 1,
            Index: &pb.Index {
                Size: 256,
                Metric: "Euclidean",
                Options: &pb.Index_Btree {
                    Btree: &pb.BtreeIndexOptions {
                        NumTrees: 15,
                        MaxLeafItems: 1024,
                    },
                },
            },
        },
        zk: suite.zk,
        cluster: suite.cluster,
        storage: suite.storage,
        partitions: make(map[string]Partition),
        localPartitions: utils.NewThreadSafeSet(),
    }
}

func (suite *DatasetTestSuite) TearDownTest() {
    suite.mockController.Finish()
}

func (suite *DatasetTestSuite) TestLoadPartitions() {
    suite.zk.EXPECT().Children("datasets/foo/partitions").Return([]string{}, nil)

    suite.dataset.loadPartitions()
}

func TestDatasetTestSuite(t *testing.T) {
    suite.Run(t, new(DatasetTestSuite))
}
