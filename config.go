package tau

import (
    "fmt";
)

type Config struct {
    IndexPath string
    Server ServerConfig
    Zookeeper ZookeeperConfig
}

type ServerConfig struct {
    Address string
    Port string
}

type ZookeeperConfig struct {
    Nodes []string
    BasePath string
}

func NewConfig() *Config {
    return &Config {
        IndexPath: "/tmp",
        Server: ServerConfig {
            Address: "",
            Port: "5555",
        },
        Zookeeper: ZookeeperConfig {
            Nodes: []string{"127.0.0.1:2181"},
            BasePath: "/tau",
        },
    }
}

func (c *Config) BindAddress() string {
    return fmt.Sprintf("%s:%s", c.Server.Address, c.Server.Port)
}
