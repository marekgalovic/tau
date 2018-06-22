package tau

import (
    "github.com/marekgalovic/tau/utils"
)

type SearchResultItem struct {
    Id int
    Distance float64
}

type SearchResult []SearchResultItem

func newSearchResult(index Index, query []float64, ids utils.Set) SearchResult {
    result := make(SearchResult, 0, ids.Len())

    for idx := range ids.ToIterator() {
        result = append(result, SearchResultItem {
            Id: idx.(int),
            Distance: index.ComputeDistance(index.Get(idx.(int)), query),
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
