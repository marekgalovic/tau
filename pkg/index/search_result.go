package index

import (
    "github.com/marekgalovic/tau/pkg/math";
    pb "github.com/marekgalovic/tau/pkg/protobuf";
    "github.com/marekgalovic/tau/pkg/utils";
)

type SearchResult []*pb.SearchResultItem

func newSearchResult(index *baseIndex, query math.Vector, ids utils.Set) SearchResult {
    result := make(SearchResult, 0, ids.Len())

    for idx := range ids.ToIterator() {
        result = append(result, &pb.SearchResultItem {
            Id: idx.(int64),
            Distance: float32(index.space.Distance(index.Get(idx.(int64)), query)),
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
