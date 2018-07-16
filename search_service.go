package tau

import (
    "fmt";

    "github.com/marekgalovic/tau/dataset";
    "github.com/marekgalovic/tau/math";
    pb "github.com/marekgalovic/tau/protobuf";
)

type searchService struct {
    datasetsManager dataset.Manager
}

func newSearchService(datasetsManager dataset.Manager) *searchService {
    return &searchService{
        datasetsManager: datasetsManager,
    }
}

func (service *searchService) Search(req *pb.SearchRequest, stream pb.SearchService_SearchServer) error {
    dataset, exists := service.datasetsManager.GetDataset(req.GetDatasetName())
    if !exists {
        return fmt.Errorf("Dataset `%s` not found.", req.GetDatasetName())
    }

    results, err := dataset.Search(req.GetK(), math.Vector(req.GetQuery()))
    if err != nil {
        return err
    }

    for _, item := range results {
        if err := stream.Send(item); err != nil {
            return err
        }
    }

    return nil
}

func (service *searchService) SearchPartitions(req *pb.SearchPartitionsRequest, stream pb.SearchService_SearchPartitionsServer) error {
    dataset, exists := service.datasetsManager.GetDataset(req.GetDatasetName())
    if !exists {
        return fmt.Errorf("Dataset `%s` not found.", req.GetDatasetName())
    }

    results, err := dataset.SearchPartitions(req.GetK(), math.Vector(req.GetQuery()), req.GetPartitions())
    if err != nil {
        return err
    }

    for _, item := range results {
        if err := stream.Send(item); err != nil {
            return err
        }
    }

    return nil
}
