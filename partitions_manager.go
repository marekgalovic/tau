package tau

import (
    "path/filepath";

    "github.com/samuel/go-zookeeper/zk";
    log "github.com/Sirupsen/logrus";
)

type PartitionsManager interface {
    Stop()
}

type partitionsManager struct {
    stop chan struct{}
    dataset Dataset
    zk *zk.Conn
    cluster Cluster
}

func newPartitionsManager(dataset Dataset, zkConn *zk.Conn, cluster Cluster) PartitionsManager {
    pm := &partitionsManager {
        stop: make(chan struct{}),
        dataset: dataset,
        zk: zkConn,
        cluster: cluster,
    }

    go pm.watchPartitions()

    return pm
}

func (pm *partitionsManager) Stop() {
    close(pm.stop)
}

func (pm *partitionsManager) watchPartitions() {
    for {
        partitions, _, event, err := pm.zk.ChildrenW(filepath.Join(pm.dataset.Meta().GetZkPath(), "partitions"))
        if err != nil {
            panic(err)
        }
        pm.updatePartitions(partitions)

        select {
        case <- event:
            continue
        case <- pm.stop:
            return
        }
    }
}

func (pm *partitionsManager) updatePartitions(partitions []string) {
    log.Info(pm.dataset.Meta().GetName(), partitions)
}

