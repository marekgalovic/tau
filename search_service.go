package tau

import (
    "github.com/marekgalovic/tau/math";
    pb "github.com/marekgalovic/tau/protobuf";
)

type searchService struct {
    datasetsManager DatasetsManager
}

func newSearchService(datasetsManager DatasetsManager) *searchService {
    return &searchService{
        datasetsManager: datasetsManager,
    }
}

func (service *searchService) Search(req *pb.SearchRequest, stream pb.SearchService_SearchServer) error {
    dataset, err := service.datasetsManager.GetDataset(req.GetDataset())
    if err != nil {
        return err
    }

    results, err := dataset.Search(1, math.VectorFromSlice(req.GetQuery()))
    if err != nil {
        return err
    }
    for _, item := range results {
        if err = stream.Send(item); err != nil {
            return err
        }
    }
    
    return nil
}
