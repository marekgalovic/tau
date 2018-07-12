package tau

import (
    "os"
    "fmt";
    "flag";
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
    c := &Config {
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

    c.parseFlags()
    return c
}

func (c *Config) parseFlags() {
    flagSet := flag.NewFlagSet("Tau Server", flag.ExitOnError)
    flagSet.StringVar(&c.Server.Address, "address", c.Server.Address, "Server address")
    flagSet.StringVar(&c.Server.Port, "port", c.Server.Port, "Server port")
    flagSet.StringVar(&c.IndexPath, "index-path", c.IndexPath, "Index path")
    flagSet.Parse(os.Args[1:])
}

func (c *Config) BindAddress() string {
    return fmt.Sprintf("%s:%s", c.Server.Address, c.Server.Port)
}
