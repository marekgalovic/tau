package tau

import (
    "fmt";
)

type Config struct {
    Server ServerConfig
    Zookeeper ZookeeperConfig
}

type ServerConfig struct {
    Address string
    Port int
}

type ZookeeperConfig struct {
    Nodes []string
    BasePath string
}

func NewConfig() *Config {
    return &Config {
        Server: ServerConfig {
            Address: "",
            Port: 5555,
        },
        Zookeeper: ZookeeperConfig {
            Nodes: []string{"127.0.0.1:2181"},
            BasePath: "/tau",
        },
    }
}

func (c *Config) BindAddress() string {
    return fmt.Sprintf("%s:%d", c.Server.Address, c.Server.Port)
}
