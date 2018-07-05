package utils

import (
    "fmt";
    "regexp";
    "strconv";
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
