package utils

import (
    "fmt";
    "time";
    "regexp";
    "strconv";
    "strings";
    "path/filepath";
    "context";
    "errors";

    "github.com/samuel/go-zookeeper/zk";
    log "github.com/Sirupsen/logrus";
)

var SeqIdRegexp = regexp.MustCompile(`\-n(\d+)`)

const (
    EventZkWatchInit int32 = 0
    EventZkNodeCreated int32 = 1
    EventZkNodeDeleted int32 = 2
)

type ChildrenChangedEvent struct {
    Type int32
    ZNode string
}

func ParseSeqId(znode string) (int64, error) {
    seqIdMatch := SeqIdRegexp.FindStringSubmatch(znode)
    if len(seqIdMatch) != 2 {
        return 0, fmt.Errorf("Invalid znode `%s`", znode)
    }

    return strconv.ParseInt(seqIdMatch[1], 10, 64)
}

type Zookeeper interface {
    Close()
    Children(string) ([]string, error)
    ChildrenW(string) ([]string, <-chan zk.Event, error)
    ChildrenChanges(context.Context, string) (<-chan ChildrenChangedEvent, <- chan error)
    Create(string, []byte, int32) (string, error)
    CreateProtectedEphemeralSequential(string, []byte) (string, error)
    CreatePath(string, []byte, int32) (string, error)
    Delete(string) error
    DeleteRecursive(string) error
    Exists(string) (bool, error)
    ExistsW(string) (bool, <-chan zk.Event, error)
    Get(string) ([]byte, error)
    GetW(string) ([]byte, <-chan zk.Event, error)
    Multi(... interface{}) error
}

type ZookeeperConfig struct {
    Nodes []string
    Timeout time.Duration
    BasePath string
}

type zookeeper struct {
    config ZookeeperConfig
    conn *zk.Conn   
}

func NewZookeeper(config ZookeeperConfig) (*zookeeper, error) {
    conn, _, err := zk.Connect(config.Nodes, config.Timeout)
    if err != nil {
        return nil, err
    }

    return &zookeeper {
        config: config,
        conn: conn,
    }, nil 
}

func (z *zookeeper) Close() {
    z.conn.Close()
}

func (z *zookeeper) Children(path string) ([]string, error) {
    children, _, err := z.conn.Children(z.withBasePath(path))

    return children, err
}

func (z *zookeeper) ChildrenW(path string) ([]string, <-chan zk.Event, error) {
    children, _, event, err := z.conn.ChildrenW(z.withBasePath(path))

    return children, event, err
}

func (z *zookeeper) ChildrenChanges(ctx context.Context, path string) (<-chan ChildrenChangedEvent, <-chan error) {
    changes := make(chan ChildrenChangedEvent)
    errors := make(chan error)

    go func(changes chan ChildrenChangedEvent, errors chan error) {
        children := NewSet()
        initialized := false
        for {
            fetchedChildren, event, err := z.ChildrenW(path)
            if err != nil {
                errors <- err
                return
            }

            updatedChildren := NewSet()
            for _, child := range fetchedChildren {
                updatedChildren.Add(child)
                if !children.Contains(child) {
                    children.Add(child)
                    var eventType int32
                    if initialized { 
                        eventType = EventZkNodeCreated
                    } else { 
                        eventType = EventZkWatchInit
                    }
                    changes <- ChildrenChangedEvent {
                        Type: eventType,
                        ZNode: child,
                    }
                }
            }

            for _, child := range children.ToSlice() {
                if !updatedChildren.Contains(child) {
                    children.Remove(child)
                    changes <- ChildrenChangedEvent {
                        Type: EventZkNodeDeleted,
                        ZNode: child.(string),
                    }
                }
            }
            initialized = true

            select {
            case <- ctx.Done():
                close(changes)
                close(errors)
                return
            case <- event:
            }
        }
    }(changes, errors)

    return changes, errors
}

func (z *zookeeper) Create(path string, data []byte, flags int32) (string, error) {
    return z.conn.Create(z.withBasePath(path), data, flags, zk.WorldACL(zk.PermAll))
}

func (z *zookeeper) CreateProtectedEphemeralSequential(path string, data []byte) (string, error) {
    return z.conn.CreateProtectedEphemeralSequential(z.withBasePath(path), data, zk.WorldACL(zk.PermAll))
}

func (z *zookeeper) CreatePath(path string, data []byte, flags int32) (string, error) {
    pathParts := strings.Split(strings.TrimLeft(z.withBasePath(path), "/"), "/")

    requests := make([]interface{}, 0)
    for i := 1; i <= len(pathParts); i++ {
        partialPath := fmt.Sprintf("/%s", filepath.Join(pathParts[:i]...))
        exists, _, err := z.conn.Exists(partialPath)
        if err != nil {
            return "", err
        }
        if exists {
            continue
        }

        request := &zk.CreateRequest{Path: partialPath, Data: nil, Flags: int32(0), Acl: zk.WorldACL(zk.PermAll )}
        if i == len(pathParts){
            request.Data = data
            request.Flags = flags
        }
        requests = append(requests, request)
    }

    if len(requests) == 0 {
        return "", zk.ErrNodeExists
    }

    responses, err := z.conn.Multi(requests...)
    if err != nil {
        return "", err
    }
    return responses[len(responses) - 1].String, nil
}

func (z *zookeeper) Delete(path string) error {
    return z.conn.Delete(z.withBasePath(path), -1)
}

func (z *zookeeper) DeleteRecursive(path string) error {
    requests, err := z.deleteRequests(z.withBasePath(path))
    if err != nil {
        return err
    }

    _, err = z.conn.Multi(requests...)
    return err
}

func (z *zookeeper) deleteRequests(path string) ([]interface{}, error) {
    children, _, err := z.conn.Children(path)
    if err != nil {
        return nil, err
    }

    requests := []interface{}{
        &zk.DeleteRequest{Path: path, Version: -1},
    }
    for _, child := range children {
        childRequests, err := z.deleteRequests(filepath.Join(path, child))
        if err != nil {
            return nil, err
        }
        requests = append(childRequests, requests...)
    }
    return requests, nil
}

func (z *zookeeper) Exists(path string) (bool, error) {
    exists, _, err := z.conn.Exists(z.withBasePath(path))

    return exists, err
}

func (z *zookeeper) ExistsW(path string) (bool, <-chan zk.Event, error) {
    exists, _, event, err := z.conn.ExistsW(z.withBasePath(path))

    return exists, event, err
}

func (z *zookeeper) Get(path string) ([]byte, error) {
    data, _, err := z.conn.Get(z.withBasePath(path))

    return data, err
}

func (z *zookeeper) GetW(path string) ([]byte, <-chan zk.Event, error) {
    data, _, event, err := z.conn.GetW(z.withBasePath(path))

    return data, event, err
}

func (z *zookeeper) Multi(requests ...interface{}) error {
    requestsWithBasePath := make([]interface{}, len(requests))

    for i, request := range requests {
        log.Info(request)
        switch request.(type) {
        case *zk.CreateRequest:
            r := request.(*zk.CreateRequest)
            r.Path = z.withBasePath(r.Path)
            requestsWithBasePath[i] = r
        case *zk.DeleteRequest:
            r := request.(*zk.DeleteRequest)
            r.Path = z.withBasePath(r.Path)
            requestsWithBasePath[i] = r
        case *zk.SetDataRequest:
            r := request.(*zk.SetDataRequest)
            r.Path = z.withBasePath(r.Path)
            requestsWithBasePath[i] = r
        case *zk.CheckVersionRequest:
            r := request.(*zk.CheckVersionRequest)
            r.Path = z.withBasePath(r.Path)
            requestsWithBasePath[i] = r
        default:
            return errors.New("Invalid request type")
        }
    }

    _, err := z.conn.Multi(requestsWithBasePath...)
    return err
}

func (z *zookeeper) withBasePath(path string) string {
    return filepath.Join(z.config.BasePath, path)
}
