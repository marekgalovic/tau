package storage

import (
    "fmt";
    "io";
    "context";
    "regexp";
    "strings";
    "path/filepath";

    gcs "cloud.google.com/go/storage";
    "google.golang.org/api/option";
    "google.golang.org/api/iterator";
)

var gcsBucketRegexp = regexp.MustCompile(`([\w\-\.]+)\/(.*)`)

type googleCloudStorage struct {
    ctx context.Context
    client *gcs.Client
}

func NewGCS(options ...option.ClientOption) (Storage, error) {
    ctx := context.Background()
    client, err := gcs.NewClient(ctx, options...)
    if err != nil {
        return nil, err
    }

    return &googleCloudStorage{
        ctx: ctx,
        client: client,
    }, nil
}

func (s *googleCloudStorage) Exists(path string) (bool, error) {
    object, err := s.objectAtPath(path)
    if err != nil {
        return false, err
    }

    _, err = object.Attrs(s.ctx)
    return err == nil, nil
}

func (s *googleCloudStorage) ListFiles(path string) ([]string, error) {
    bucketName, objectsPath, err := s.normalizePath(path)
    if err != nil {
        return nil, err
    }

    prefix := objectsPath
    wildcard := isWildcard(objectsPath)
    if wildcard {
        prefix = filepath.Dir(prefix)
    } 
    if !strings.HasSuffix(prefix, "/") {
        prefix += "/"
    }

    objectIterator := s.client.Bucket(bucketName).Objects(s.ctx, &gcs.Query{
        Delimiter: "/",
        Prefix: prefix,
        Versions: false,
    })

    result := make([]string, 0)
    for {
        object, err := objectIterator.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return nil, err
        }
        if wildcard {
            match, err := filepath.Match(objectsPath, object.Name)
            if err != nil {
                return nil, err
            }
            if !match {
                continue
            }
        }
        result = append(result, "gs://" + filepath.Join(bucketName, object.Name))
    }
    return result, nil
}

func (s *googleCloudStorage) Reader(path string) (io.ReadCloser, error) {
    object, err := s.objectAtPath(path)
    if err != nil {
        return nil, err
    }

    return object.NewReader(s.ctx)
}

func (s *googleCloudStorage) Writer(path string) (io.WriteCloser, error) {
    object, err := s.objectAtPath(path)
    if err != nil {
        return nil, err
    }

    return object.NewWriter(s.ctx), nil
}

func (s *googleCloudStorage) objectAtPath(path string) (*gcs.ObjectHandle, error) {
    bucketName, objectName, err := s.normalizePath(path)
    if err != nil {
        return nil, err
    }

    return s.client.Bucket(bucketName).Object(objectName), nil
}

func (gcs *googleCloudStorage) normalizePath(path string) (string, string, error) {
    path = strings.TrimPrefix(path, "gs://")
    pathParts := gcsBucketRegexp.FindStringSubmatch(path)

    if len(pathParts) != 3 {
        return "", "", fmt.Errorf("Invalid path `%s`", path)
    }
    return pathParts[1], pathParts[2], nil
}
