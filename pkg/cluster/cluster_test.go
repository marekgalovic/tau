package cluster

import (
    "fmt";
    "time";
    "strings";
    "testing";

    "github.com/marekgalovic/tau/pkg/utils";

    "github.com/stretchr/testify/suite";
    "github.com/satori/go.uuid";
)

type ClusterTestSuite struct {
    suite.Suite
    zk utils.Zookeeper
}

func (suite *ClusterTestSuite) SetupTest() {
    randId, err := uuid.NewV4()
    if err != nil {
        suite.Fail(fmt.Sprintf("%s", err))
    }
    basePath, err := randId.MarshalText()
    if err != nil {
        suite.Fail(fmt.Sprintf("%s", err))
    }

    suite.zk, err = utils.NewZookeeper(utils.ZookeeperConfig {
        Nodes: []string{"127.0.0.1:2181"},
        Timeout: 1 * time.Second,
        BasePath: fmt.Sprintf("/%s", strings.Replace(string(basePath), "-", "", -1)),
    })
    if err != nil {
        suite.Fail(fmt.Sprintf("%s", err))
    }
}

func (suite *ClusterTestSuite) TearDownTest() {
    suite.zk.Close()
}

func (suite *ClusterTestSuite) TestClusterConstructorBootstrapsZnodes() {
    exists, err := suite.zk.Exists("nodes")
    suite.Nil(err)
    suite.False(exists)

    cluster, err := NewCluster(ClusterConfig{Uuid: "aaa", Ip: "127.0.0.1", Port: "5555"}, suite.zk)
    suite.Nil(err)
    suite.NotNil(cluster)

    exists, err = suite.zk.Exists("nodes")
    suite.Nil(err)
    suite.True(exists)
}

func (suite *ClusterTestSuite) TestClusterRegistersNodeToZookeeper() {
    cluster, err := NewCluster(ClusterConfig{Uuid: "aaa", Ip: "127.0.0.1", Port: "5555"}, suite.zk)
    suite.Nil(err)

    exists, err := suite.zk.Exists(fmt.Sprintf("nodes/%s", cluster.Uuid()))
    suite.Nil(err)
    suite.True(exists)
}

func (suite *ClusterTestSuite) TestClusterNotifiesNodeChanges() {
    cluster, err := NewCluster(ClusterConfig{Uuid: "aaa", Ip: "127.0.0.1", Port: "5555"}, suite.zk)
    suite.Nil(err)

    changes := cluster.NodeChanges() 
    select {
    case event := <-changes:
        suite.Equal(EventNodeCreated, event.(*NodesChangedNotification).Event)
        suite.Equal("aaa", event.(*NodesChangedNotification).Node.Meta().GetUuid())
    case <-time.After(1 * time.Second):
        suite.Fail("Watch timed out")
    }

    suite.Nil(suite.zk.Delete("nodes/aaa"))

    select {
    case event := <-changes:
        suite.Equal(EventNodeDeleted, event.(*NodesChangedNotification).Event)
        suite.Equal("aaa", event.(*NodesChangedNotification).Node.Meta().GetUuid())
    case <-time.After(1 * time.Second):
        suite.Fail("Watch timed out")
    }
}

func (suite *ClusterTestSuite) TestListNodes() {
    cluster, err := NewCluster(ClusterConfig{Uuid: "aaa", Ip: "127.0.0.1", Port: "5555"}, suite.zk)
    suite.Nil(err)

    nodes, err := cluster.ListNodes()
    suite.Nil(err)
    suite.Equal(1, len(nodes))
}

func (suite *ClusterTestSuite) TestGetNode() {
    c, err := NewCluster(ClusterConfig{Uuid: "aaa", Ip: "127.0.0.1", Port: "5555"}, suite.zk)
    suite.Nil(err)

    suite.Equal(0, len(c.(*cluster).nodes))

    node, err := c.GetNode("aaa")
    suite.Nil(err)
    suite.NotNil(node)
    suite.Equal("aaa", node.Meta().GetUuid())

    suite.Equal(1, len(c.(*cluster).nodes))
}

func (suite *ClusterTestSuite) TestGetHrwNode() {
    c, err := NewCluster(ClusterConfig{Uuid: "aaa", Ip: "127.0.0.1", Port: "5555"}, suite.zk)
    suite.Nil(err)

    _, err = NewCluster(ClusterConfig{Uuid: "bbb", Ip: "127.0.0.1", Port: "5556"}, suite.zk)
    suite.Nil(err)

    _, err = NewCluster(ClusterConfig{Uuid: "ccc", Ip: "127.0.0.1", Port: "5557"}, suite.zk)

    <- time.After(100 * time.Millisecond)  // Give nodes watch some time to update local cache
    suite.Equal(3, len(c.(*cluster).nodes))
    
    node, err := c.GetHrwNode("foo")
    suite.Nil(err)
    suite.Equal("ccc", node.Meta().GetUuid())
}

func (suite *ClusterTestSuite) TestGetTopHrwNodes() {
    c, err := NewCluster(ClusterConfig{Uuid: "aaa", Ip: "127.0.0.1", Port: "5555"}, suite.zk)
    suite.Nil(err)

    _, err = NewCluster(ClusterConfig{Uuid: "bbb", Ip: "127.0.0.1", Port: "5556"}, suite.zk)
    suite.Nil(err)

    _, err = NewCluster(ClusterConfig{Uuid: "ccc", Ip: "127.0.0.1", Port: "5557"}, suite.zk)

    <- time.After(100 * time.Millisecond)  // Give nodes watch some time to update local cache
    suite.Equal(3, len(c.(*cluster).nodes))

    nodes, err := c.GetTopHrwNodes(2, "ccc")
    suite.Nil(err)
    suite.Equal(2, nodes.Len())
    suite.True(nodes.Contains("ccc"))
    suite.True(nodes.Contains("aaa"))
    suite.False(nodes.Contains("bbb"))
}

func TestClusterTestSuite(t *testing.T) {
    suite.Run(t, new(ClusterTestSuite))
}
