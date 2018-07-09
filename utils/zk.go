package utils

import (
    "fmt";
    "regexp";
    "strconv";
    "strings";
    "path/filepath";

    "github.com/samuel/go-zookeeper/zk";
)

var SeqIdRegexp = regexp.MustCompile(`\-n(\d+)`)

func ParseSeqId(znode string) (int64, error) {
    seqIdMatch := SeqIdRegexp.FindStringSubmatch(znode)
    if len(seqIdMatch) != 2 {
        return 0, fmt.Errorf("Invalid znode `%s`", znode)
    }

    return strconv.ParseInt(seqIdMatch[1], 10, 64)
}

func ZkCreatePath(zkConn *zk.Conn, path string, data []byte, flags int32, acl []zk.ACL) error {
    pathParts := strings.Split(strings.TrimLeft(path, "/"), "/")

    requests := make([]interface{}, 0)
    for i := 1; i <= len(pathParts); i++ {
        partialPath := fmt.Sprintf("/%s", filepath.Join(pathParts[:i]...))
        exists, _, err := zkConn.Exists(partialPath)
        if err != nil {
            return err
        }
        if exists {
            continue
        }

        request := &zk.CreateRequest{Path: partialPath, Data: nil, Flags: flags, Acl: acl}
        if i == len(pathParts){
            request.Data = data
        }
        requests = append(requests, request)
    }

    if len(requests) == 0 {
        return nil
    }

    _, err := zkConn.Multi(requests...)
    return err
}

func deleteRequests(zkConn *zk.Conn, path string) ([]interface{}, error) {
    children, _, err := zkConn.Children(path)
    if err != nil {
        return nil, err
    }

    requests := []interface{}{
        &zk.DeleteRequest{Path: path, Version: -1},
    }
    for _, child := range children {
        childRequests, err := deleteRequests(zkConn, filepath.Join(path, child))
        if err != nil {
            return nil, err
        }
        requests = append(childRequests, requests...)
    }
    return requests, nil
}

func ZkRecursiveDelete(zkConn *zk.Conn, path string) error {
    requests, err := deleteRequests(zkConn, path)
    if err != nil {
        return err
    }

    _, err = zkConn.Multi(requests...)
    return err
}
