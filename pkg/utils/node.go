package utils

import (
    "fmt";
    "net";
    "os";
    "os/user";
    "path/filepath";

    "github.com/satori/go.uuid";
)


func VolatileNodeUuid() (string, error) {
    uuid, err := uuid.NewV1()
    if err != nil {
        return "", err
    }
    uuidBytes, err := uuid.MarshalText()
    if err != nil {
        return "", err
    }
    return string(uuidBytes), nil    
}

func NodeUuid() (string, error) {
    usr, err := user.Current()
    if err != nil {
        return "", nil
    }

    nodeIdFilePath := filepath.Join(usr.HomeDir, ".tau_node_id")
    if _, err := os.Stat(nodeIdFilePath); err != nil {
        nodeIdFile, err := os.Create(nodeIdFilePath);
        if err != nil {
            return "", err
        }

        uuid, err := uuid.NewV1()
        if err != nil {
            return "", err        }
        uuidBytes, err := uuid.MarshalText()
        if err != nil {
            return "", err
        }
        if _, err := nodeIdFile.Write(uuidBytes); err != nil {
            return "", err
        }
        if err := nodeIdFile.Close(); err != nil {
            return "", err
        }
        return uuid.String(), nil
    }

    nodeIdFile, err := os.Open(nodeIdFilePath)
    if err != nil {
        return "", err
    }
    uuidBytes := make([]byte, 36)
    if _, err := nodeIdFile.Read(uuidBytes); err != nil {
        return "", err
    }
    nodeIdFile.Close()

    return string(uuidBytes), nil
}

func NodeIpAddress() (string, error) {
    interfaces, err := net.Interfaces()
    if err != nil {
        return "", err
    }
    
    for _, iface := range interfaces {
        if iface.Flags & net.FlagUp == 0 {
            continue
        }
        if iface.Flags & net.FlagLoopback != 0 {
            continue
        }

        addrs, err := iface.Addrs()
        if err != nil {
            return "", err
        }

        for _, addr := range addrs {
            var ip net.IP
            switch v := addr.(type) {
                case *net.IPNet:
                    ip = v.IP
                case *net.IPAddr:
                    ip = v.IP
            }

            if ip == nil || ip.IsLoopback() {
                continue
            }
            ip = ip.To4()
            if ip == nil {
                continue
            }

            return ip.String(), nil
        }
    }

    return "", fmt.Errorf("Unable to find node IP.")
}
