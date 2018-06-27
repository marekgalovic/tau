package tau

import (
    "fmt";

    "github.com/marekgalovic/tau/index";
)

type DatasetsManager interface {
    Add(string, index.Index) error
    Get(string) (index.Index, error)
    Delete(string) error
}

type datasetsManager struct {
    datasets map[string]index.Index
}

func NewDatasetsManager() DatasetsManager {
    return &datasetsManager {
        datasets: make(map[string]index.Index),
    }
}

func (m *datasetsManager) Add(name string, index index.Index) error {
    if _, exists := m.datasets[name]; exists {
        return fmt.Errorf("Dataset `%s` already exists", name)
    }

    m.datasets[name] = index
    return nil
}

func (m *datasetsManager) Get(name string) (index.Index, error) {
    index, exists := m.datasets[name]
    if !exists {
        return nil, fmt.Errorf("Index `%s` does not exist", name)
    }

    return index, nil
}

func (m *datasetsManager) Delete(name string) error {
    if _, exists := m.datasets[name]; !exists {
        return fmt.Errorf("Index `%s` does not exist", name)
    }

    delete(m.datasets, name)
    return nil
}
