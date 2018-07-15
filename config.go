package tau

import (
    "os"
    "fmt";
    "flag";
    "time";

    "github.com/marekgalovic/tau/dataset";
    "github.com/marekgalovic/tau/utils";
)

type Config struct {
    Server ServerConfig
    Dataset dataset.DatasetManagerConfig
    Zookeeper utils.ZookeeperConfig
}

type ServerConfig struct {
    Address string
    Port string
}

func NewConfig() *Config {
    c := &Config {
        Server: ServerConfig {
            Address: "",
            Port: "5555",
        },
        Dataset: dataset.DatasetManagerConfig {
            IndicesPath: "/tmp",
        },
        Zookeeper: utils.ZookeeperConfig {
            Nodes: []string{"127.0.0.1:2181"},
            Timeout: 4 * time.Second,
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
    flagSet.StringVar(&c.Dataset.IndicesPath, "indices-path", c.Dataset.IndicesPath, "Index path")
    flagSet.Parse(os.Args[1:])
}

func (c *Config) BindAddress() string {
    return fmt.Sprintf("%s:%s", c.Server.Address, c.Server.Port)
}
