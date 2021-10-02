package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stdiopt/parquet/internal/parquet"
)

// Writer encodes parquet.
type Writer struct {
	SchemaHandler *SchemaHandler
	NP            int64 // parallel number
	Footer        *parquet.FileMetaData
	PFile         File

	PageSize        int64
	RowGroupSize    int64
	CompressionType parquet.CompressionCodec
	Offset          int64

	Objs              []interface{}
	ObjsSize          int64
	ObjSize           int64
	CheckSizeCritical int64

	PagesMapBuf map[string][]*layoutPage
	Size        int64
	NumRows     int64

	DictRecs map[string]*layoutDictRecType

	ColumnIndexes []*parquet.ColumnIndex
	OffsetIndexes []*parquet.OffsetIndex

	MarshalFunc func(src []interface{}, sh *SchemaHandler) (*map[string]*layoutTable, error)
}

// NewWriter create a parquet handler. Obj is a object with tags or JSON schema string.
func NewWriter(pFile File, obj interface{}, np int64) (*Writer, error) {
	var err error

	var schemaHandler *SchemaHandler
	var schema []*parquet.SchemaElement
	if obj != nil {
		switch v := obj.(type) {
		case string:
			schemaHandler, err = NewSchemaHandlerFromJSON(v)
			if err != nil {
				return nil, err
			}
		case []*parquet.SchemaElement:
			schemaHandler = NewSchemaHandlerFromSchemaList(v)
		default:
			schemaHandler, err = NewSchemaHandlerFromStruct(obj)
			if err != nil {
				return nil, err
			}
		}
	}
	schema = append(schema, schemaHandler.SchemaElements...)

	createdBy := "parquet-go version latest"
	res := Writer{
		SchemaHandler:   schemaHandler,
		NP:              np,
		PageSize:        8 * 1024,          // 8K
		RowGroupSize:    128 * 1024 * 1024, // 128M
		CompressionType: parquet.CompressionCodec_SNAPPY,
		ObjsSize:        0,
		Offset:          4,
		PFile:           pFile,
		PagesMapBuf:     make(map[string][]*layoutPage),
		DictRecs:        make(map[string]*layoutDictRecType),
		Footer: &parquet.FileMetaData{
			Version:   1,
			CreatedBy: &createdBy,
			Schema:    schema,
		},
		ColumnIndexes: make([]*parquet.ColumnIndex, 0),
		OffsetIndexes: make([]*parquet.OffsetIndex, 0),
		MarshalFunc:   marshal,
	}

	_, err = res.PFile.Write([]byte("PAR1"))

	return &res, err
}

// RenameSchema renames schema name to exname in tags.
func (pw *Writer) RenameSchema() {
	for i := 0; i < len(pw.Footer.Schema); i++ {
		pw.Footer.Schema[i].Name = pw.SchemaHandler.Infos[i].ExName
	}
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			inPathStr := pathToStr(chunk.MetaData.PathInSchema)
			exPathStr := pw.SchemaHandler.InPathToExPath[inPathStr]
			exPath := strToPath(exPathStr)[1:]
			chunk.MetaData.PathInSchema = exPath
		}
	}
}

// Close flushes and closes the parquet file.
func (pw *Writer) Close() error {
	var err error

	if err = pw.Flush(true); err != nil {
		return err
	}
	ts := thrift.NewTSerializer()
	// ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	ts.Protocol = thrift.NewTBinaryProtocolConf(ts.Transport, nil)
	pw.RenameSchema()

	// write ColumnIndex
	idx := 0
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			columnIndexBuf, err := ts.Write(context.TODO(), pw.ColumnIndexes[idx])
			if err != nil {
				return err
			}
			if _, err = pw.PFile.Write(columnIndexBuf); err != nil {
				return err
			}

			idx++

			pos := pw.Offset
			columnChunk.ColumnIndexOffset = &pos
			columnIndexBufSize := int32(len(columnIndexBuf))
			columnChunk.ColumnIndexLength = &columnIndexBufSize

			pw.Offset += int64(columnIndexBufSize)
		}
	}

	// write OffsetIndex
	idx = 0
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			offsetIndexBuf, err := ts.Write(context.TODO(), pw.OffsetIndexes[idx])
			if err != nil {
				return err
			}
			if _, err = pw.PFile.Write(offsetIndexBuf); err != nil {
				return err
			}

			idx++

			pos := pw.Offset
			columnChunk.OffsetIndexOffset = &pos
			offsetIndexBufSize := int32(len(offsetIndexBuf))
			columnChunk.OffsetIndexLength = &offsetIndexBufSize

			pw.Offset += int64(offsetIndexBufSize)
		}
	}

	footerBuf, err := ts.Write(context.TODO(), pw.Footer)
	if err != nil {
		return err
	}

	if _, err = pw.PFile.Write(footerBuf); err != nil {
		return err
	}
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))

	if _, err = pw.PFile.Write(footerSizeBuf); err != nil {
		return err
	}
	if _, err = pw.PFile.Write([]byte("PAR1")); err != nil {
		return err
	}
	return nil
}

// Write one object to parquet file.
func (pw *Writer) Write(src interface{}) error {
	ln := int64(len(pw.Objs))

	val := reflect.ValueOf(src)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		src = val.Interface()
	}

	if pw.CheckSizeCritical <= ln {
		pw.ObjSize = (pw.ObjSize+SizeOf(val))/2 + 1
	}
	pw.ObjsSize += pw.ObjSize
	pw.Objs = append(pw.Objs, src)

	criSize := pw.NP * pw.PageSize * pw.SchemaHandler.GetColumnNum()

	if pw.ObjsSize >= criSize {
		return pw.Flush(false)
	}
	dln := (criSize - pw.ObjsSize + pw.ObjSize - 1) / pw.ObjSize / 2
	pw.CheckSizeCritical = dln + ln
	return nil
}

func (pw *Writer) flushObjs() error {
	var err error
	l := int64(len(pw.Objs))
	if l <= 0 {
		return nil
	}
	pagesMapList := make([]map[string][]*layoutPage, pw.NP)
	for i := 0; i < int(pw.NP); i++ {
		pagesMapList[i] = make(map[string][]*layoutPage)
	}

	var c int64
	delta := (l + pw.NP - 1) / pw.NP
	lock := new(sync.Mutex)
	var wg sync.WaitGroup
	errs := make([]error, pw.NP)

	for c = 0; c < pw.NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > l {
			end = l
		}
		if bgn >= l {
			bgn, end = l, l
		}

		wg.Add(1)
		go func(b, e int, index int64) {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					switch x := r.(type) {
					case string:
						errs[index] = errors.New(x)
					case error:
						errs[index] = x
					default:
						errs[index] = errors.New("unknown error")
					}
				}
			}()

			if e <= b {
				return
			}

			tableMap, err2 := pw.MarshalFunc(pw.Objs[b:e], pw.SchemaHandler)
			if err2 != nil {
				errs[index] = err2
				return
			}

			for name, table := range *tableMap {
				if table.Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY ||
					table.Info.Encoding == parquet.Encoding_RLE_DICTIONARY {
					func() {
						if pw.NP > 1 {
							lock.Lock()
							defer lock.Unlock()
						}
						if _, ok := pw.DictRecs[name]; !ok {
							pw.DictRecs[name] = newLayoutDictRec(*table.Schema.Type)
						}
						pagesMapList[index][name], _, err = layoutTableToDictDataPages(pw.DictRecs[name],
							table, int32(pw.PageSize), 32, pw.CompressionType)
					}()
					continue
				}
				pagesMapList[index][name], _, err = layoutTableToDataPages(
					table,
					int32(pw.PageSize),
					pw.CompressionType,
				)
				if err != nil {
					errs[index] = err
					return
				}
			}
		}(int(bgn), int(end), c)
	}

	wg.Wait()

	for _, e := range errs {
		if e != nil {
			err = e
			break
		}
	}

	for _, pagesMap := range pagesMapList {
		for name, pages := range pagesMap {
			if _, ok := pw.PagesMapBuf[name]; !ok {
				pw.PagesMapBuf[name] = pages
			} else {
				pw.PagesMapBuf[name] = append(pw.PagesMapBuf[name], pages...)
			}
			for _, page := range pages {
				pw.Size += int64(len(page.RawData))
				page.DataTable = nil // release memory
			}
		}
	}

	pw.NumRows += int64(len(pw.Objs))
	return err
}

// Flush the write buffer to parquet file.
func (pw *Writer) Flush(flag bool) error {
	var err error

	if err = pw.flushObjs(); err != nil {
		return err
	}

	if (pw.Size+pw.ObjsSize >= pw.RowGroupSize || flag) && len(pw.PagesMapBuf) > 0 {
		// pages -> chunk
		chunkMap := make(map[string]*layoutChunk)
		for name, pages := range pw.PagesMapBuf {
			if len(pages) == 0 || (pages[0].Info.Encoding != parquet.Encoding_PLAIN_DICTIONARY &&
				pages[0].Info.Encoding != parquet.Encoding_RLE_DICTIONARY) {
				chunkMap[name] = layoutPagesToChunk(pages)
			}

			dictPage, _, err := layoutDictRecToDictPage(pw.DictRecs[name], int32(pw.PageSize), pw.CompressionType)
			if err != nil {
				return err
			}
			tmp := append([]*layoutPage{dictPage}, pages...)
			chunkMap[name] = layoutPagesToDictChunk(tmp)
		}

		pw.DictRecs = make(map[string]*layoutDictRecType) // clean records for next chunks

		// chunks -> rowGroup
		rowGroup := newLayoutRowGroup()
		rowGroup.RowGroupHeader.Columns = make([]*parquet.ColumnChunk, 0)

		for k := 0; k < len(pw.SchemaHandler.SchemaElements); k++ {
			// for _, chunk := range chunkMap {
			schema := pw.SchemaHandler.SchemaElements[k]
			if schema.GetNumChildren() > 0 {
				continue
			}
			chunk := chunkMap[pw.SchemaHandler.IndexMap[int32(k)]]
			if chunk == nil {
				continue
			}
			rowGroup.Chunks = append(rowGroup.Chunks, chunk)
			// rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalCompressedSize
			rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalUncompressedSize
			rowGroup.RowGroupHeader.Columns = append(rowGroup.RowGroupHeader.Columns, chunk.ChunkHeader)
		}
		rowGroup.RowGroupHeader.NumRows = pw.NumRows
		pw.NumRows = 0

		for k := 0; k < len(rowGroup.Chunks); k++ {
			rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = -1
			rowGroup.Chunks[k].ChunkHeader.FileOffset = pw.Offset

			pageCount := len(rowGroup.Chunks[k].Pages)

			// add ColumnIndex
			columnIndex := parquet.NewColumnIndex()
			columnIndex.NullPages = make([]bool, pageCount)
			columnIndex.MinValues = make([][]byte, pageCount)
			columnIndex.MaxValues = make([][]byte, pageCount)
			columnIndex.BoundaryOrder = parquet.BoundaryOrder_UNORDERED
			pw.ColumnIndexes = append(pw.ColumnIndexes, columnIndex)

			// add OffsetIndex
			offsetIndex := parquet.NewOffsetIndex()
			offsetIndex.PageLocations = make([]*parquet.PageLocation, 0)
			pw.OffsetIndexes = append(pw.OffsetIndexes, offsetIndex)

			firstRowIndex := int64(0)

			for l := 0; l < pageCount; l++ {
				if rowGroup.Chunks[k].Pages[l].Header.Type == parquet.PageType_DICTIONARY_PAGE {
					tmp := pw.Offset
					rowGroup.Chunks[k].ChunkHeader.MetaData.DictionaryPageOffset = &tmp
				} else if rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset <= 0 {
					rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = pw.Offset
				}

				page := rowGroup.Chunks[k].Pages[l]
				// only record DataPage
				if page.Header.Type != parquet.PageType_DICTIONARY_PAGE {
					if page.Header.DataPageHeader == nil && page.Header.DataPageHeaderV2 == nil {
						panic(errors.New("unsupported data page: " + page.Header.String()))
					}

					var minVal []byte
					var maxVal []byte
					if page.Header.DataPageHeader != nil && page.Header.DataPageHeader.Statistics != nil {
						minVal = page.Header.DataPageHeader.Statistics.Min
						maxVal = page.Header.DataPageHeader.Statistics.Max
					} else if page.Header.DataPageHeaderV2 != nil && page.Header.DataPageHeaderV2.Statistics != nil {
						minVal = page.Header.DataPageHeaderV2.Statistics.Min
						maxVal = page.Header.DataPageHeaderV2.Statistics.Max
					}

					columnIndex.MinValues[l] = minVal
					columnIndex.MaxValues[l] = maxVal

					pageLocation := parquet.NewPageLocation()
					pageLocation.Offset = pw.Offset
					pageLocation.FirstRowIndex = firstRowIndex
					pageLocation.CompressedPageSize = page.Header.CompressedPageSize

					offsetIndex.PageLocations = append(offsetIndex.PageLocations, pageLocation)

					firstRowIndex += int64(page.Header.DataPageHeader.NumValues)
				}

				data := rowGroup.Chunks[k].Pages[l].RawData
				if _, err = pw.PFile.Write(data); err != nil {
					return err
				}
				pw.Offset += int64(len(data))
			}
		}

		pw.Footer.RowGroups = append(pw.Footer.RowGroups, rowGroup.RowGroupHeader)
		pw.Size = 0
		pw.PagesMapBuf = make(map[string][]*layoutPage)
	}
	pw.Footer.NumRows += int64(len(pw.Objs))
	pw.Objs = pw.Objs[:0]
	pw.ObjsSize = 0
	return nil
}
