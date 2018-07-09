package tau

import (
    "context";
    "path/filepath";

    "github.com/samuel/go-zookeeper/zk";
    log "github.com/Sirupsen/logrus";
)

type PartitionsManager interface {
    Stop()
}

type partitionsManager struct {
    ctx context.Context
    cancel context.CancelFunc

    dataset Dataset
    zk *zk.Conn
    cluster Cluster
}

func newPartitionsManager(ctx context.Context, dataset Dataset, zkConn *zk.Conn, cluster Cluster) PartitionsManager {
    ctx, cancel := context.WithCancel(ctx)

    pm := &partitionsManager {
        ctx: ctx,
        cancel: cancel,
        dataset: dataset,
        zk: zkConn,
        cluster: cluster,
    }

    go pm.watchPartitions()
    go pm.watchNodes()

    return pm
}

func (pm *partitionsManager) Stop() {
    pm.cancel()
}

func (pm *partitionsManager) watchPartitions() {
    for {
        partitions, _, event, err := pm.zk.ChildrenW(filepath.Join(pm.dataset.Meta().GetZkPath(), "partitions"))
        if err == zk.ErrNoNode {
            log.Warn(err)
            return
        }
        if err != nil {
            panic(err)
        }
        pm.updatePartitions(partitions)

        select {
        case <- event:
            continue
        case <- pm.ctx.Done():
            return
        }
    }
}

func (pm *partitionsManager) watchNodes() {
    notifications := pm.cluster.NodeChanges()

    for {
        select {
        case notification := <- notifications:
            notification = notification.(*NodesChangedNotification)
            log.Info("PM Nodes Update", notification)
            continue
        case <- pm.ctx.Done():
            return
        }
    }
}

func (pm *partitionsManager) updatePartitions(partitions []string) {
    log.Info(partitions)
    // log.Info(pm.dataset.Meta().GetName(), partitions)
}

