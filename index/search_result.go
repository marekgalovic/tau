package index

import (
    "github.com/marekgalovic/tau/math";
    pb "github.com/marekgalovic/tau/protobuf";
    "github.com/marekgalovic/tau/utils";
)

type SearchResult []*pb.SearchResultItem

func newSearchResult(index Index, query math.Vector, ids utils.Set) SearchResult {
    result := make(SearchResult, 0, ids.Len())

    for idx := range ids.ToIterator() {
        result = append(result, &pb.SearchResultItem {
            Id: idx.(int64),
            Distance: float32(index.ComputeDistance(index.Get(idx.(int64)), query)),
        })
    }

    return result
}

func (r SearchResult) Len() int {
    return len(r)
}

func (r SearchResult) Swap(i, j int) {
    r[i], r[j] = r[j], r[i]
}

func (r SearchResult) Less(i, j int) bool {
    return r[i].Distance < r[j].Distance
}
