package storage

import (
    "strings";
)

func isWildcard(path string) bool {
    return strings.Contains(path, "*") || strings.Contains(path, "?")
}
