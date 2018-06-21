package tau

type SearchResultItem struct {
    Id int
    Distance float32
}

type SearchResult []SearchResultItem

func newSearchResult(index Index, query []float32, ids []int) SearchResult {
    result := make(SearchResult, len(ids))

    for i, idx := range ids {
        result[i] = SearchResultItem {
            Id: idx,
            Distance: index.ComputeDistance(index.Get(idx), query),
        }
    }

    return result
}

func newSearchResultFromSet(index Index, query []float32, ids map[int]struct{}) SearchResult {
    result := make(SearchResult, 0, len(ids))
    
    for idx, _ := range ids {
        result = append(result, SearchResultItem {
            Id: idx,
            Distance: index.ComputeDistance(index.Get(idx), query),
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
