package parquet

import (
	"fmt"
)

// NewParquetColumnReader creates a parquet column reader.
func NewParquetColumnReader(pFile File, np int64) (*Reader, error) {
	res := new(Reader)
	res.NP = np
	res.PFile = pFile
	if err := res.ReadFooter(); err != nil {
		return nil, err
	}
	res.ColumnBuffers = make(map[string]*columnBufferType)
	res.SchemaHandler = NewSchemaHandlerFromSchemaList(res.Footer.GetSchema())
	res.RenameSchema()

	return res, nil
}

// SkipRowsByPath skip certain rows.
func (pr *Reader) SkipRowsByPath(pathStr string, num int64) error {
	errPathNotFound := fmt.Errorf("path %v not found", pathStr)

	pathStr, err := pr.SchemaHandler.ConvertToInPathStr(pathStr)
	if num <= 0 || len(pathStr) == 0 || err != nil {
		return err
	}

	if _, ok := pr.SchemaHandler.MapIndex[pathStr]; !ok {
		return errPathNotFound
	}

	if _, ok := pr.ColumnBuffers[pathStr]; !ok {
		var err error
		if pr.ColumnBuffers[pathStr], err = newColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr); err != nil {
			return err
		}
	}

	cb, ok := pr.ColumnBuffers[pathStr]
	if !ok {
		return errPathNotFound
	}
	cb.SkipRows(num)
	return nil
}

// SkipRowsByIndex skip rows by index.
func (pr *Reader) SkipRowsByIndex(index int64, num int64) error {
	if index >= int64(len(pr.SchemaHandler.ValueColumns)) {
		return nil
	}
	pathStr := pr.SchemaHandler.ValueColumns[index]
	return pr.SkipRowsByPath(pathStr, num)
}

// ReadColumnByPath reads column by path in schema.
func (pr *Reader) ReadColumnByPath(
	pathStr string,
	num int64,
) (values []interface{}, rls []int32, dls []int32, err error) {
	errPathNotFound := fmt.Errorf("path %v not found", pathStr)

	pathStr, err = pr.SchemaHandler.ConvertToInPathStr(pathStr)
	if num <= 0 || len(pathStr) == 0 || err != nil {
		return []interface{}{}, []int32{}, []int32{}, err
	}

	if _, ok := pr.SchemaHandler.MapIndex[pathStr]; !ok {
		return []interface{}{}, []int32{}, []int32{}, errPathNotFound
	}

	if _, ok := pr.ColumnBuffers[pathStr]; !ok {
		var err error
		if pr.ColumnBuffers[pathStr], err = newColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr); err != nil {
			return []interface{}{}, []int32{}, []int32{}, err
		}
	}

	if cb, ok := pr.ColumnBuffers[pathStr]; ok {
		table, _ := cb.ReadRows(num)
		return table.Values, table.RepetitionLevels, table.DefinitionLevels, nil
	}
	return []interface{}{}, []int32{}, []int32{}, errPathNotFound
}

// ReadColumnByIndex reads column by index. The index of first column is 0.
func (pr *Reader) ReadColumnByIndex(
	index int64,
	num int64,
) (values []interface{}, rls []int32, dls []int32, err error) {
	if index >= int64(len(pr.SchemaHandler.ValueColumns)) {
		err = fmt.Errorf("index %v out of range %v", index, len(pr.SchemaHandler.ValueColumns))
		return
	}
	pathStr := pr.SchemaHandler.ValueColumns[index]
	return pr.ReadColumnByPath(pathStr, num)
}
