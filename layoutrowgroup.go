package parquet

import (
	"github.com/stdiopt/parquet/internal/parquet"
)

// layoutRowGroup stores the layoutRowGroup in parquet file.
type layoutRowGroup struct {
	Chunks         []*layoutChunk
	RowGroupHeader *parquet.RowGroup
}

// newLayoutRowGroup returns a new row group.
func newLayoutRowGroup() *layoutRowGroup {
	return &layoutRowGroup{
		RowGroupHeader: parquet.NewRowGroup(),
	}
}

// RowGroupToTableMap converts a RowGroup to table map.
func (rowGroup *layoutRowGroup) RowGroupToTableMap() *map[string]*layoutTable {
	tableMap := make(map[string]*layoutTable)
	for _, chunk := range rowGroup.Chunks {
		pathStr := ""
		for _, page := range chunk.Pages {
			if pathStr == "" {
				pathStr = pathToStr(page.DataTable.Path)
			}
			if _, ok := tableMap[pathStr]; !ok {
				tableMap[pathStr] = newLayoutTableFromTable(page.DataTable)
			}
			tableMap[pathStr].Merge(page.DataTable)
		}
	}
	return &tableMap
}
