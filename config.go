package tau

import (
    "os"
    "fmt";
    "flag";
    "time";
    "strings";

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

func splitItems(items string) []string {
    splitted := make([]string, 0)
    for _, item := range strings.Split(items, ",") {
        if len(item) == 0 {
            continue
        }
        splitted = append(splitted, strings.Trim(item, " "))
    }
    return splitted
}

func NewConfig() (*Config, error) {
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

    if err := c.parseFlags(); err != nil {
        return nil, err
    }
    return c, nil
}

func (c *Config) parseFlags() error {
    var zkNodes string

    flagSet := flag.NewFlagSet("Tau Server", flag.ExitOnError)
    flagSet.StringVar(&c.Server.Address, "address", c.Server.Address, "Server address")
    flagSet.StringVar(&c.Server.Port, "port", c.Server.Port, "Server port")
    flagSet.StringVar(&c.Dataset.IndicesPath, "indices-path", c.Dataset.IndicesPath, "Index path")
    flagSet.StringVar(&zkNodes, "zk-nodes", "127.0.0.1:2181", "Comma separated list of zookeeper servers")
    flagSet.StringVar(&c.Zookeeper.BasePath, "zk-base-path", c.Zookeeper.BasePath, "Zookeeper base path")
    flagSet.Parse(os.Args[1:])

    c.Zookeeper.Nodes = splitItems(zkNodes)
    if len(c.Zookeeper.Nodes) == 0 {
        return fmt.Errorf("No zookeeper nodes provided")
    }
    return nil
}

func (c *Config) BindAddress() string {
    return fmt.Sprintf("%s:%s", c.Server.Address, c.Server.Port)
}
