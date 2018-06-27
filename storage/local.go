package storage

import (
    "fmt";
    "os";
    "io";
    "io/ioutil";
    "path/filepath"
)

type localStorage struct {}

func NewLocal() Storage {
    return &localStorage{}
}

func (s *localStorage) Exists(path string) bool {
    if isWildcard(path) {
        files, err := ioutil.ReadDir(filepath.Dir(path))
        if err != nil {
            panic(err)
        }
        pattern := filepath.Base(path)
        for _, file := range files {
            match, err := filepath.Match(pattern, file.Name())
            if err != nil {
                panic(err)
            }
            if match {
                return true
            }
        }
        return false
    }

    _, err := os.Stat(path)
    return err == nil
}

func (s *localStorage) ListFiles(path string) ([]string, error) {
    if !s.Exists(path) {
        return nil, fmt.Errorf("Path `%s` does not exist", path)
    }
    if isWildcard(path) {
        dirPath := filepath.Dir(path)
        files, err := ioutil.ReadDir(dirPath)
        if err != nil {
            return nil, err
        }

        pattern := filepath.Base(path)
        result := make([]string, 0)
        for _, file := range files {
            match, err := filepath.Match(pattern, file.Name())
            if err != nil {
                panic(err)
            }
            if match {
                result = append(result, filepath.Join(dirPath, file.Name()))
            }
        }
        return result, nil
    }
    fileInfo, err := os.Stat(path)
    if err != nil {
        return nil, err
    }

    if !fileInfo.IsDir() {
        return []string{path}, nil
    }

    files, err := ioutil.ReadDir(path)
    if err != nil {
        return nil, err
    }

    result := make([]string, 0)
    for _, file := range files {
        result = append(result, filepath.Join(path, file.Name()))
    }
    return result,nil
}

func (s *localStorage) Reader(path string) (io.ReadCloser, error) {
    return os.Open(path)
}

func (s *localStorage) Writer(path string) (io.WriteCloser, error) {
    return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
}
