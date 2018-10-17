package storage

import (
    "io";
)

type Storage interface {
    Exists(string) (bool, error)
    ListFiles(string) ([]string, error)
    Reader(string) (io.ReadCloser, error)
    Writer(string) (io.WriteCloser, error)
}
