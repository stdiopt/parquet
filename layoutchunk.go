package parquet

import (
	"github.com/stdiopt/parquet/internal/encoding"
	"github.com/stdiopt/parquet/internal/parquet"
)

// layoutChunk stores the ColumnChunk in parquet file.
type layoutChunk struct {
	Pages       []*layoutPage
	ChunkHeader *parquet.ColumnChunk
}

// Convert several pages to one chunk.
func layoutPagesToChunk(pages []*layoutPage) *layoutChunk {
	ln := len(pages)
	var numValues int64
	var totalUncompressedSize int64
	var totalCompressedSize int64

	maxVal := pages[0].MaxVal
	minVal := pages[0].MinVal
	var nullCount int64
	pT := pages[0].Schema.Type
	cT := pages[0].Schema.ConvertedType
	logT := pages[0].Schema.LogicalType
	omitStats := pages[0].Info.OmitStats
	funcTable := findFuncTable(pT, cT, logT)

	for i := 0; i < ln; i++ {
		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) +
			int64(len(pages[i].RawData)) -
			int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		if !omitStats {
			minVal = min(funcTable, minVal, pages[i].MinVal)
			maxVal = max(funcTable, maxVal, pages[i].MaxVal)
			nullCount += *pages[i].NullCount
		}
	}

	chunk := new(layoutChunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = *pages[0].Schema.Type
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_BIT_PACKED)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN)
	// metaData.Encodings = append(metaData.Encodings, parquet.Encoding_DELTA_BINARY_PACKED)
	metaData.Codec = pages[0].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[0].Path
	metaData.Statistics = parquet.NewStatistics()

	if !omitStats && maxVal != nil && minVal != nil {
		tmpBufMax := encoding.WritePlain([]interface{}{maxVal}, *pT)
		tmpBufMin := encoding.WritePlain([]interface{}{minVal}, *pT)
		if *pT == parquet.Type_BYTE_ARRAY {
			tmpBufMax = tmpBufMax[4:]
			tmpBufMin = tmpBufMin[4:]
		}
		metaData.Statistics.Max = tmpBufMax
		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.MaxValue = tmpBufMax
		metaData.Statistics.MinValue = tmpBufMin
	}

	if !omitStats {
		metaData.Statistics.NullCount = &nullCount
	}

	chunk.ChunkHeader.MetaData = metaData
	return chunk
}

// Convert several pages to one chunk with dict page first.
func layoutPagesToDictChunk(pages []*layoutPage) *layoutChunk {
	if len(pages) < 2 {
		return nil
	}
	var numValues int64
	var totalUncompressedSize int64
	var totalCompressedSize int64

	maxVal := pages[1].MaxVal
	minVal := pages[1].MinVal
	var nullCount int64
	pT := pages[1].Schema.Type
	cT := pages[1].Schema.ConvertedType
	logT := pages[1].Schema.LogicalType
	omitStats := pages[0].Info.OmitStats
	funcTable := findFuncTable(pT, cT, logT)

	for i := 0; i < len(pages); i++ {
		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else if pages[i].Header.DataPageHeaderV2 != nil {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) +
			int64(len(pages[i].RawData)) -
			int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		if !omitStats && i > 0 {
			minVal = min(funcTable, minVal, pages[i].MinVal)
			maxVal = max(funcTable, maxVal, pages[i].MaxVal)
			nullCount += *pages[i].NullCount
		}
	}

	chunk := new(layoutChunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = *pages[1].Schema.Type
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_BIT_PACKED)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN_DICTIONARY)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE_DICTIONARY)

	metaData.Codec = pages[1].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[1].Path
	metaData.Statistics = parquet.NewStatistics()

	if !omitStats && maxVal != nil && minVal != nil {
		tmpBufMax := encoding.WritePlain([]interface{}{maxVal}, *pT)
		tmpBufMin := encoding.WritePlain([]interface{}{minVal}, *pT)
		if *pT == parquet.Type_BYTE_ARRAY {
			tmpBufMax = tmpBufMax[4:]
			tmpBufMin = tmpBufMin[4:]
		}
		metaData.Statistics.Max = tmpBufMax
		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.MaxValue = tmpBufMax
		metaData.Statistics.MinValue = tmpBufMin
	}

	if !omitStats {
		metaData.Statistics.NullCount = &nullCount
	}

	chunk.ChunkHeader.MetaData = metaData
	return chunk
}

// Decode a dict chunk
/*func layoutDecodeDictChunk(chunk *layoutChunk) {
	dictPage := chunk.Pages[0]
	numPages := len(chunk.Pages)
	for i := 1; i < numPages; i++ {
		numValues := len(chunk.Pages[i].DataTable.Values)
		for j := 0; j < numValues; j++ {
			if chunk.Pages[i].DataTable.Values[j] != nil {
				index := chunk.Pages[i].DataTable.Values[j].(int64)
				chunk.Pages[i].DataTable.Values[j] = dictPage.DataTable.Values[index]
			}
		}
	}
	chunk.Pages = chunk.Pages[1:] // delete the head dict page
}*/

// Read one chunk from parquet file (Deprecated)
/*func layoutReadChunk(
	thriftReader *thrift.TBufferedTransport,
	schemaHandler *SchemaHandler,
	chunkHeader *parquet.ColumnChunk,
) (*layoutChunk, error) {
	chunk := new(layoutChunk)
	chunk.ChunkHeader = chunkHeader

	var readValues int64
	numValues := chunkHeader.MetaData.GetNumValues()
	for readValues < numValues {
		page, cnt, _, err := readPage(thriftReader, schemaHandler, chunkHeader.GetMetaData())
		if err != nil {
			return nil, err
		}
		chunk.Pages = append(chunk.Pages, page)
		readValues += cnt
	}

	if len(chunk.Pages) > 0 && chunk.Pages[0].Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		layoutDecodeDictChunk(chunk)
	}
	return chunk, nil
}*/
