package parquet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/bits"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/stdiopt/parquet/internal/compress"
	"github.com/stdiopt/parquet/internal/encoding"
	"github.com/stdiopt/parquet/internal/parquet"
)

// layoutPage is used to store the page data.
type layoutPage struct {
	// Header of a page
	Header *parquet.PageHeader
	// Table to store values
	DataTable *layoutTable
	// Compressed data of the page, which is written in parquet file
	RawData []byte
	// Compress type: gzip/snappy/zstd/none
	CompressType parquet.CompressionCodec
	// Schema
	Schema *parquet.SchemaElement
	// Path in schema(include the root)
	Path []string
	// Maximum of the values
	MaxVal interface{}
	// Minimum of the values
	MinVal interface{}
	// NullCount
	NullCount *int64
	// Tag info
	Info *Tag

	PageSize int32
}

// Create a new page.
func newLayoutPage() *layoutPage {
	page := new(layoutPage)
	page.DataTable = nil
	page.Header = parquet.NewPageHeader()
	page.Info = NewTag()
	page.PageSize = 8 * 1024
	return page
}

// Create a new dict page.
func newLayoutDictPage() *layoutPage {
	page := newLayoutPage()
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	page.PageSize = 8 * 1024
	return page
}

// Create a new data page..
func newLayoutDataPage() *layoutPage {
	page := newLayoutPage()
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.PageSize = 8 * 1024
	return page
}

// Convert a table to data pages.
func layoutTableToDataPages(
	table *layoutTable,
	pageSize int32,
	compressType parquet.CompressionCodec,
) ([]*layoutPage, int64, error) {
	var totSize int64
	totalLn := len(table.Values)
	res := make([]*layoutPage, 0)
	i := 0
	pT := table.Schema.Type
	cT := table.Schema.ConvertedType
	logT := table.Schema.LogicalType
	omitStats := table.Info.OmitStats

	for i < totalLn {
		j := i + 1
		var size int32
		var numValues int32

		maxVal := table.Values[i]
		minVal := table.Values[i]
		nullCount := int64(0)

		funcTable := findFuncTable(pT, cT, logT)

		for j < totalLn && size < pageSize {
			if table.DefinitionLevels[j] == table.MaxDefinitionLevel {
				numValues++
				var elSize int32
				if omitStats {
					_, _, elSize = funcTable.MinMaxSize(nil, nil, table.Values[j])
				} else {
					minVal, maxVal, elSize = funcTable.MinMaxSize(minVal, maxVal, table.Values[j])
				}
				size += elSize
			}
			if table.Values[j] == nil {
				nullCount++
			}
			j++
		}

		page := newLayoutDataPage()
		page.PageSize = pageSize
		page.Header.DataPageHeader.NumValues = numValues
		page.Header.Type = parquet.PageType_DATA_PAGE

		page.DataTable = new(layoutTable)
		page.DataTable.RepetitionType = table.RepetitionType
		page.DataTable.Path = table.Path
		page.DataTable.MaxDefinitionLevel = table.MaxDefinitionLevel
		page.DataTable.MaxRepetitionLevel = table.MaxRepetitionLevel
		page.DataTable.Values = table.Values[i:j]
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:j]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:j]
		if !omitStats {
			page.MaxVal = maxVal
			page.MinVal = minVal
			page.NullCount = &nullCount
		}
		page.Schema = table.Schema
		page.CompressType = compressType
		page.Path = table.Path
		page.Info = table.Info

		_, err := page.DataPageCompress(compressType)
		if err != nil {
			return nil, 0, err
		}

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = j
	}
	return res, totSize, nil
}

// Decode dict page.
func (p *layoutPage) Decode(dictPage *layoutPage) {
	if dictPage == nil || p == nil ||
		(p.Header.DataPageHeader == nil && p.Header.DataPageHeaderV2 == nil) {
		return
	}

	if p.Header.DataPageHeader != nil &&
		(p.Header.DataPageHeader.Encoding != parquet.Encoding_RLE_DICTIONARY &&
			p.Header.DataPageHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY) {
		return
	}

	if p.Header.DataPageHeaderV2 != nil &&
		(p.Header.DataPageHeaderV2.Encoding != parquet.Encoding_RLE_DICTIONARY &&
			p.Header.DataPageHeaderV2.Encoding != parquet.Encoding_PLAIN_DICTIONARY) {
		return
	}

	numValues := len(p.DataTable.Values)
	for i := 0; i < numValues; i++ {
		if p.DataTable.Values[i] != nil {
			index := p.DataTable.Values[i].(int64)
			p.DataTable.Values[i] = dictPage.DataTable.Values[index]
		}
	}
}

// Encoding values.
func (p *layoutPage) EncodingValues(valuesBuf []interface{}) []byte {
	encodingMethod := parquet.Encoding_PLAIN
	if p.Info.Encoding != 0 {
		encodingMethod = p.Info.Encoding
	}
	switch encodingMethod {
	case parquet.Encoding_RLE:
		bitWidth := p.Info.Length
		return encoding.WriteRLEBitPackedHybrid(valuesBuf, bitWidth, *p.Schema.Type)
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return encoding.WriteDelta(valuesBuf)
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return encoding.WriteDeltaByteArray(valuesBuf)
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return encoding.WriteDeltaLengthByteArray(valuesBuf)
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		return encoding.WriteByteStreamSplit(valuesBuf)
	default:
		return encoding.WritePlain(valuesBuf, *p.Schema.Type)
	}
}

// Compress the data page to parquet file.
func (p *layoutPage) DataPageCompress(compressType parquet.CompressionCodec) ([]byte, error) {
	ln := len(p.DataTable.DefinitionLevels)

	// values////////////////////////////////////////////
	// valuesBuf == nil means "up to i, every item in DefinitionLevels was
	// MaxDefinitionLevel". This lets us avoid allocating the array for the
	// (somewhat) common case of "all values present".
	var valuesBuf []interface{}
	for i := 0; i < ln; i++ {
		if p.DataTable.DefinitionLevels[i] == p.DataTable.MaxDefinitionLevel {
			if valuesBuf != nil {
				valuesBuf = append(valuesBuf, p.DataTable.Values[i])
			}
		} else if valuesBuf == nil {
			valuesBuf = make([]interface{}, i, ln)
			copy(valuesBuf[:i], p.DataTable.Values[:i])
		}
	}
	if valuesBuf == nil {
		valuesBuf = p.DataTable.Values
	}
	// valuesRawBuf := encoding.WritePlain(valuesBuf)
	valuesRawBuf := p.EncodingValues(valuesBuf)

	// definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if p.DataTable.MaxDefinitionLevel > 0 {
		definitionLevelBuf = encoding.WriteRLEBitPackedHybridInt32(
			p.DataTable.DefinitionLevels,
			int32(bits.Len32(uint32(p.DataTable.MaxDefinitionLevel))))
	}

	// repetitionLevel/////////////////////////////////
	var repetitionLevelBuf []byte
	if p.DataTable.MaxRepetitionLevel > 0 {
		repetitionLevelBuf = encoding.WriteRLEBitPackedHybridInt32(
			p.DataTable.RepetitionLevels,
			int32(bits.Len32(uint32(p.DataTable.MaxRepetitionLevel))))
	}

	// dataBuf = repetitionBuf + definitionBuf + valuesRawBuf
	dataBuf := make([]byte, 0, len(repetitionLevelBuf)+len(definitionLevelBuf)+len(valuesRawBuf))
	dataBuf = append(dataBuf, repetitionLevelBuf...)
	dataBuf = append(dataBuf, definitionLevelBuf...)
	dataBuf = append(dataBuf, valuesRawBuf...)

	dataEncodeBuf, err := compress.Compress(dataBuf, compressType)
	if err != nil {
		return nil, err
	}

	// pageHeader/////////////////////////////////////
	p.Header = parquet.NewPageHeader()
	p.Header.Type = parquet.PageType_DATA_PAGE
	p.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	p.Header.UncompressedPageSize = int32(len(dataBuf))
	p.Header.DataPageHeader = parquet.NewDataPageHeader()
	p.Header.DataPageHeader.NumValues = int32(len(p.DataTable.DefinitionLevels))
	p.Header.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	p.Header.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	p.Header.DataPageHeader.Encoding = p.Info.Encoding

	p.Header.DataPageHeader.Statistics = parquet.NewStatistics()
	if p.MaxVal != nil {
		tmpBuf := encoding.WritePlain([]interface{}{p.MaxVal}, *p.Schema.Type)
		if *p.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		p.Header.DataPageHeader.Statistics.Max = tmpBuf
		p.Header.DataPageHeader.Statistics.MaxValue = tmpBuf
	}
	if p.MinVal != nil {
		tmpBuf := encoding.WritePlain([]interface{}{p.MinVal}, *p.Schema.Type)
		if *p.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		p.Header.DataPageHeader.Statistics.Min = tmpBuf
		p.Header.DataPageHeader.Statistics.MinValue = tmpBuf
	}

	p.Header.DataPageHeader.Statistics.NullCount = p.NullCount

	ts := thrift.NewTSerializer()
	// ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	ts.Protocol = thrift.NewTCompactProtocolConf(ts.Transport, nil)
	pageHeaderBuf, _ := ts.Write(context.TODO(), p.Header)

	res := append(pageHeaderBuf, dataEncodeBuf...) // nolint gocritic
	p.RawData = res

	return res, nil
}

// Compress data page v2 to parquet file.
func (p *layoutPage) DataPageV2Compress(compressType parquet.CompressionCodec) ([]byte, error) {
	ln := len(p.DataTable.DefinitionLevels)

	// values////////////////////////////////////////////
	valuesBuf := make([]interface{}, 0)
	for i := 0; i < ln; i++ {
		if p.DataTable.DefinitionLevels[i] == p.DataTable.MaxDefinitionLevel {
			valuesBuf = append(valuesBuf, p.DataTable.Values[i])
		}
	}
	// valuesRawBuf := encoding.WritePlain(valuesBuf)
	valuesRawBuf := p.EncodingValues(valuesBuf)

	// definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if p.DataTable.MaxDefinitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = int64(p.DataTable.DefinitionLevels[i])
		}
		definitionLevelBuf = encoding.WriteRLE(numInterfaces,
			int32(bits.Len32(uint32(p.DataTable.MaxDefinitionLevel))),
			parquet.Type_INT64)
	}

	// repetitionLevel/////////////////////////////////
	r0Num := int32(0)
	var repetitionLevelBuf []byte
	if p.DataTable.MaxRepetitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = int64(p.DataTable.RepetitionLevels[i])
			if p.DataTable.RepetitionLevels[i] == 0 {
				r0Num++
			}
		}
		repetitionLevelBuf = encoding.WriteRLE(numInterfaces,
			int32(bits.Len32(uint32(p.DataTable.MaxRepetitionLevel))),
			parquet.Type_INT64)
	}

	dataEncodeBuf, err := compress.Compress(valuesRawBuf, compressType)
	if err != nil {
		return nil, err
	}

	// pageHeader/////////////////////////////////////
	p.Header = parquet.NewPageHeader()
	p.Header.Type = parquet.PageType_DATA_PAGE_V2
	p.Header.CompressedPageSize = int32(len(dataEncodeBuf) + len(definitionLevelBuf) + len(repetitionLevelBuf))
	p.Header.UncompressedPageSize = int32(len(valuesRawBuf) + len(definitionLevelBuf) + len(repetitionLevelBuf))
	p.Header.DataPageHeaderV2 = parquet.NewDataPageHeaderV2()
	p.Header.DataPageHeaderV2.NumValues = int32(len(p.DataTable.Values))
	p.Header.DataPageHeaderV2.NumNulls = p.Header.DataPageHeaderV2.NumValues - int32(len(valuesBuf))
	p.Header.DataPageHeaderV2.NumRows = r0Num
	// page.Header.DataPageHeaderV2.Encoding = parquet.Encoding_PLAIN
	p.Header.DataPageHeaderV2.Encoding = p.Info.Encoding

	p.Header.DataPageHeaderV2.DefinitionLevelsByteLength = int32(len(definitionLevelBuf))
	p.Header.DataPageHeaderV2.RepetitionLevelsByteLength = int32(len(repetitionLevelBuf))
	p.Header.DataPageHeaderV2.IsCompressed = true

	p.Header.DataPageHeaderV2.Statistics = parquet.NewStatistics()
	if p.MaxVal != nil {
		tmpBuf := encoding.WritePlain([]interface{}{p.MaxVal}, *p.Schema.Type)
		if *p.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		p.Header.DataPageHeaderV2.Statistics.Max = tmpBuf
		p.Header.DataPageHeaderV2.Statistics.MaxValue = tmpBuf
	}
	if p.MinVal != nil {
		tmpBuf := encoding.WritePlain([]interface{}{p.MinVal}, *p.Schema.Type)
		if *p.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		p.Header.DataPageHeaderV2.Statistics.Min = tmpBuf
		p.Header.DataPageHeaderV2.Statistics.MinValue = tmpBuf
	}

	p.Header.DataPageHeaderV2.Statistics.NullCount = p.NullCount

	ts := thrift.NewTSerializer()
	// ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	ts.Protocol = thrift.NewTBinaryProtocolConf(ts.Transport, nil)
	pageHeaderBuf, _ := ts.Write(context.TODO(), p.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, repetitionLevelBuf...)
	res = append(res, definitionLevelBuf...)
	res = append(res, dataEncodeBuf...)
	p.RawData = res

	return res, nil
}

// This is a test function
/*func layoutReadPage2(
	thriftReader *thrift.TBufferedTransport,
	schemaHandler *SchemaHandler,
	colMetaData *parquet.ColumnMetaData,
) (*layoutPage, int64, int64, error) {
	var err error
	page, err := layoutReadPageRawData(thriftReader, schemaHandler, colMetaData)
	if err != nil {
		return nil, 0, 0, err
	}
	numValues, numRows, err := page.GetRLDLFromRawData(schemaHandler)
	if err != nil {
		return nil, 0, 0, err
	}
	if err = page.GetValueFromRawData(schemaHandler); err != nil {
		return page, 0, 0, err
	}
	return page, numValues, numRows, nil
}*/

// Read page RawData.
func layoutReadPageRawData(
	thriftReader *thrift.TBufferedTransport,
	schemaHandler *SchemaHandler,
	colMetaData *parquet.ColumnMetaData,
) (*layoutPage, error) {
	var err error

	pageHeader, err := layoutReadPageHeader(thriftReader)
	if err != nil {
		return nil, err
	}

	var page *layoutPage

	switch pageHeader.GetType() {
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		page = newLayoutDataPage()
	case parquet.PageType_DICTIONARY_PAGE:
		page = newLayoutDictPage()
	default:
		return page, fmt.Errorf("unsupported page type")
	}

	compressedPageSize := pageHeader.GetCompressedPageSize()
	buf := make([]byte, compressedPageSize)
	if _, err := io.ReadFull(thriftReader, buf); err != nil {
		return nil, err
	}

	page.Header = pageHeader
	page.CompressType = colMetaData.GetCodec()
	page.RawData = buf
	page.Path = make([]string, 0)
	page.Path = append(page.Path, schemaHandler.GetRootInName())
	page.Path = append(page.Path, colMetaData.GetPathInSchema()...)
	pathIndex := schemaHandler.MapIndex[pathToStr(page.Path)]
	schema := schemaHandler.SchemaElements[pathIndex]
	page.Schema = schema
	return page, nil
}

// Get RepetitionLevels and Definitions from RawData.
func (p *layoutPage) GetRLDLFromRawData(schemaHandler *SchemaHandler) (int64, int64, error) {
	var err error
	bytesReader := bytes.NewReader(p.RawData)
	buf := make([]byte, 0)

	if p.Header.GetType() == parquet.PageType_DATA_PAGE_V2 {
		dll := p.Header.DataPageHeaderV2.GetDefinitionLevelsByteLength()
		rll := p.Header.DataPageHeaderV2.GetRepetitionLevelsByteLength()
		repetitionLevelsBuf, definitionLevelsBuf := make([]byte, rll), make([]byte, dll)
		dataBuf := make([]byte, len(p.RawData)-int(rll)-int(dll))
		if _, err := bytesReader.Read(repetitionLevelsBuf); err != nil {
			return 0, 0, err
		}
		if _, err := bytesReader.Read(definitionLevelsBuf); err != nil {
			return 0, 0, err
		}
		if _, err := bytesReader.Read(dataBuf); err != nil {
			return 0, 0, err
		}

		tmpBuf := make([]byte, 0)
		if rll > 0 {
			tmpBuf = encoding.WritePlainINT32([]interface{}{rll})
			tmpBuf = append(tmpBuf, repetitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		if dll > 0 {
			tmpBuf = encoding.WritePlainINT32([]interface{}{dll})
			tmpBuf = append(tmpBuf, definitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		buf = append(buf, dataBuf...)
	} else if buf, err = compress.Uncompress(p.RawData, p.CompressType); err != nil {
		return 0, 0, fmt.Errorf("unsupported compress method")
	}

	bytesReader = bytes.NewReader(buf)
	switch p.Header.GetType() {
	case parquet.PageType_DATA_PAGE_V2, parquet.PageType_DATA_PAGE:
		var numValues uint64
		if p.Header.GetType() == parquet.PageType_DATA_PAGE {
			numValues = uint64(p.Header.DataPageHeader.GetNumValues())
		} else {
			numValues = uint64(p.Header.DataPageHeaderV2.GetNumValues())
		}

		maxDefinitionLevel, err := schemaHandler.MaxDefinitionLevel(p.Path)
		if err != nil {
			return 0, 0, err
		}
		maxRepetitionLevel, err := schemaHandler.MaxRepetitionLevel(p.Path)
		if err != nil {
			return 0, 0, err
		}

		var repetitionLevels, definitionLevels []interface{}
		if maxRepetitionLevel > 0 {
			bitWidth := uint64(bits.Len32(uint32(maxRepetitionLevel)))
			if repetitionLevels, err = layoutReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth); err != nil {
				return 0, 0, err
			}
		} else {
			repetitionLevels = make([]interface{}, numValues)
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = int64(0)
			}
		}
		if len(repetitionLevels) > int(numValues) {
			repetitionLevels = repetitionLevels[:numValues]
		}

		if maxDefinitionLevel > 0 {
			bitWidth := uint64(bits.Len32(uint32(maxDefinitionLevel)))

			definitionLevels, err = layoutReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth)
			if err != nil {
				return 0, 0, err
			}
		} else {
			definitionLevels = make([]interface{}, numValues)
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = int64(0)
			}
		}
		if len(definitionLevels) > int(numValues) {
			definitionLevels = definitionLevels[:numValues]
		}

		table := new(layoutTable)
		table.Path = p.Path
		name := pathToStr(p.Path)
		table.RepetitionType = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
		table.MaxRepetitionLevel = maxRepetitionLevel
		table.MaxDefinitionLevel = maxDefinitionLevel
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		numRows := int64(0)
		for i := 0; i < len(definitionLevels); i++ {
			dl, _ := definitionLevels[i].(int64)
			rl, _ := repetitionLevels[i].(int64)
			table.RepetitionLevels[i] = int32(rl)
			table.DefinitionLevels[i] = int32(dl)
			if table.RepetitionLevels[i] == 0 {
				numRows++
			}
		}
		p.DataTable = table
		p.RawData = buf[len(buf)-bytesReader.Len():]

		return int64(numValues), numRows, nil
	case parquet.PageType_DICTIONARY_PAGE:
		table := new(layoutTable)
		table.Path = p.Path
		p.DataTable = table
		p.RawData = buf
		return 0, 0, nil
	default:
		return 0, 0, fmt.Errorf("unsupported page type")
	}
}

// Get values from raw data.
func (p *layoutPage) GetValueFromRawData(schemaHandler *SchemaHandler) error {
	var err error
	var encodingType parquet.Encoding

	switch p.Header.GetType() {
	case parquet.PageType_DICTIONARY_PAGE:
		bytesReader := bytes.NewReader(p.RawData)
		p.DataTable.Values, err = encoding.ReadPlain(bytesReader,
			*p.Schema.Type,
			uint64(p.Header.DictionaryPageHeader.GetNumValues()),
			0)
		if err != nil {
			return err
		}
	case parquet.PageType_DATA_PAGE_V2:
		if p.RawData, err = compress.Uncompress(p.RawData, p.CompressType); err != nil {
			return err
		}
		fallthrough
	case parquet.PageType_DATA_PAGE:
		encodingType = p.Header.DataPageHeader.GetEncoding()
		bytesReader := bytes.NewReader(p.RawData)

		var numNulls uint64
		for i := 0; i < len(p.DataTable.DefinitionLevels); i++ {
			if p.DataTable.DefinitionLevels[i] != p.DataTable.MaxDefinitionLevel {
				numNulls++
			}
		}
		name := pathToStr(p.DataTable.Path)
		var values []interface{}
		var ct parquet.ConvertedType = -1
		if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
			ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
		}

		values, err = layoutReadDataPageValues(bytesReader,
			encodingType,
			*p.Schema.Type,
			ct,
			uint64(len(p.DataTable.DefinitionLevels))-numNulls,
			uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))
		if err != nil {
			return err
		}
		j := 0
		for i := 0; i < len(p.DataTable.DefinitionLevels); i++ {
			if p.DataTable.DefinitionLevels[i] == p.DataTable.MaxDefinitionLevel {
				p.DataTable.Values[i] = values[j]
				j++
			}
		}
		p.RawData = []byte{}
		return nil

	default:
		return fmt.Errorf("unsupported page type")
	}
	return nil
}

// layoutReadPageHeader read page header.
func layoutReadPageHeader(thriftReader *thrift.TBufferedTransport) (*parquet.PageHeader, error) {
	// protocol := thrift.NewTCompactProtocol(thriftReader)
	protocol := thrift.NewTCompactProtocolConf(thriftReader, nil)
	pageHeader := parquet.NewPageHeader()
	err := pageHeader.Read(context.TODO(), protocol)
	return pageHeader, err
}

// layoutReadDataPageValues read data page values.
func layoutReadDataPageValues(
	bytesReader *bytes.Reader,
	encodingMethod parquet.Encoding,
	dataType parquet.Type,
	_ parquet.ConvertedType,
	cnt uint64,
	bitWidth uint64,
) ([]interface{}, error) {
	var res []interface{}
	if cnt <= 0 {
		return res, nil
	}

	switch encodingMethod {
	case parquet.Encoding_PLAIN:
		return encoding.ReadPlain(bytesReader, dataType, cnt, bitWidth)
	case parquet.Encoding_PLAIN_DICTIONARY, parquet.Encoding_RLE_DICTIONARY:
		b, err := bytesReader.ReadByte()
		if err != nil {
			return res, err
		}
		bitWidth = uint64(b)

		buf, err := encoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, uint64(bytesReader.Len()))
		if err != nil {
			return res, err
		}
		return buf[:cnt], err

	case parquet.Encoding_RLE:
		values, err := encoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_INT32 {
			for i := 0; i < len(values); i++ {
				values[i] = int32(values[i].(int64))
			}
		}
		return values[:cnt], nil

	case parquet.Encoding_BIT_PACKED:
		// deprecated
		return res, fmt.Errorf("unsupported encoding method BIT_PACKED")
	case parquet.Encoding_DELTA_BINARY_PACKED:
		if dataType == parquet.Type_INT32 {
			return encoding.ReadDeltaBinaryPackedINT32(bytesReader)
		}
		if dataType == parquet.Type_INT64 {
			return encoding.ReadDeltaBinaryPackedINT64(bytesReader)
		}
		return res, fmt.Errorf("the encoding method DELTA_BINARY_PACKED can only be used with int32 and int64 types")

	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		values, err := encoding.ReadDeltaLengthByteArray(bytesReader)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := 0; i < len(values); i++ {
				values[i] = values[i].(string)
			}
		}
		return values[:cnt], nil

	case parquet.Encoding_DELTA_BYTE_ARRAY:
		values, err := encoding.ReadDeltaByteArray(bytesReader)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := 0; i < len(values); i++ {
				values[i] = values[i].(string)
			}
		}
		return values[:cnt], nil
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		switch dataType {
		case parquet.Type_FLOAT:
			return encoding.ReadByteStreamSplitFloat32(bytesReader, cnt)
		case parquet.Type_DOUBLE:
			return encoding.ReadByteStreamSplitFloat64(bytesReader, cnt)
		default:
			return res, fmt.Errorf("the encoding method BYTE_STREAM_SPLIT can only be used with float and double types")
		}

	default:
		return res, fmt.Errorf("unknown encoding method")
	}
}

func readPage(
	thriftReader *thrift.TBufferedTransport,
	schemaHandler *SchemaHandler,
	colMetaData *parquet.ColumnMetaData,
) (*layoutPage, int64, int64, error) {
	var err error

	pageHeader, err := layoutReadPageHeader(thriftReader)
	if err != nil {
		return nil, 0, 0, err
	}

	buf := make([]byte, 0)

	var page *layoutPage
	compressedPageSize := pageHeader.GetCompressedPageSize()

	if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		dll := pageHeader.DataPageHeaderV2.GetDefinitionLevelsByteLength()
		rll := pageHeader.DataPageHeaderV2.GetRepetitionLevelsByteLength()
		repetitionLevelsBuf := make([]byte, rll)
		definitionLevelsBuf := make([]byte, dll)
		dataBuf := make([]byte, compressedPageSize-rll-dll)

		if _, err = io.ReadFull(thriftReader, repetitionLevelsBuf); err != nil {
			return nil, 0, 0, err
		}
		if _, err = io.ReadFull(thriftReader, definitionLevelsBuf); err != nil {
			return nil, 0, 0, err
		}
		if _, err = io.ReadFull(thriftReader, dataBuf); err != nil {
			return nil, 0, 0, err
		}

		codec := colMetaData.GetCodec()
		if len(dataBuf) > 0 {
			if dataBuf, err = compress.Uncompress(dataBuf, codec); err != nil {
				return nil, 0, 0, err
			}
		}

		tmpBuf := make([]byte, 0)
		if rll > 0 {
			tmpBuf = encoding.WritePlainINT32([]interface{}{rll})
			tmpBuf = append(tmpBuf, repetitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		if dll > 0 {
			tmpBuf = encoding.WritePlainINT32([]interface{}{dll})
			tmpBuf = append(tmpBuf, definitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		buf = append(buf, dataBuf...)
	} else {
		buf = make([]byte, compressedPageSize)
		if _, err = io.ReadFull(thriftReader, buf); err != nil {
			return nil, 0, 0, err
		}
		codec := colMetaData.GetCodec()
		if buf, err = compress.Uncompress(buf, codec); err != nil {
			return nil, 0, 0, err
		}
	}

	bytesReader := bytes.NewReader(buf)
	path := make([]string, 0)
	path = append(path, schemaHandler.GetRootInName())
	path = append(path, colMetaData.GetPathInSchema()...)
	name := pathToStr(path)

	switch pageHeader.GetType() {
	case parquet.PageType_DICTIONARY_PAGE:
		page = newLayoutDictPage()
		page.Header = pageHeader
		table := new(layoutTable)
		table.Path = path
		bitWidth, idx := 0, schemaHandler.MapIndex[name]
		if colMetaData.GetType() == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			bitWidth = int(schemaHandler.SchemaElements[idx].GetTypeLength())
		}

		table.Values, err = encoding.ReadPlain(bytesReader,
			colMetaData.GetType(),
			uint64(pageHeader.DictionaryPageHeader.GetNumValues()),
			uint64(bitWidth))
		if err != nil {
			return nil, 0, 0, err
		}
		page.DataTable = table

		return page, 0, 0, nil

	case parquet.PageType_INDEX_PAGE:
		return nil, 0, 0, fmt.Errorf("unsupported page type: INDEX_PAGE")
	case parquet.PageType_DATA_PAGE_V2, parquet.PageType_DATA_PAGE:
		page = newLayoutDataPage()
		page.Header = pageHeader
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)

		var numValues uint64
		var encodingType parquet.Encoding

		if pageHeader.GetType() == parquet.PageType_DATA_PAGE {
			numValues = uint64(pageHeader.DataPageHeader.GetNumValues())
			encodingType = pageHeader.DataPageHeader.GetEncoding()
		} else {
			numValues = uint64(pageHeader.DataPageHeaderV2.GetNumValues())
			encodingType = pageHeader.DataPageHeaderV2.GetEncoding()
		}

		var repetitionLevels []interface{}
		if maxRepetitionLevel > 0 {
			bitWidth := uint64(bits.Len32(uint32(maxRepetitionLevel)))

			repetitionLevels, err = layoutReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth)
			if err != nil {
				return nil, 0, 0, err
			}
		} else {
			repetitionLevels = make([]interface{}, numValues)
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = int64(0)
			}
		}
		if len(repetitionLevels) > int(numValues) {
			repetitionLevels = repetitionLevels[:numValues]
		}

		var definitionLevels []interface{}
		if maxDefinitionLevel > 0 {
			bitWidth := uint64(bits.Len32(uint32(maxDefinitionLevel)))

			definitionLevels, err = layoutReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth)
			if err != nil {
				return nil, 0, 0, err
			}
		} else {
			definitionLevels = make([]interface{}, numValues)
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = int64(0)
			}
		}
		if len(definitionLevels) > int(numValues) {
			definitionLevels = definitionLevels[:numValues]
		}

		var numNulls uint64
		for i := 0; i < len(definitionLevels); i++ {
			if int32(definitionLevels[i].(int64)) != maxDefinitionLevel {
				numNulls++
			}
		}

		var values []interface{}
		var ct parquet.ConvertedType = -1
		if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
			ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
		}
		values, err = layoutReadDataPageValues(bytesReader,
			encodingType,
			colMetaData.GetType(),
			ct,
			uint64(len(definitionLevels))-numNulls,
			uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))
		if err != nil {
			return nil, 0, 0, err
		}

		table := new(layoutTable)
		table.Path = path
		table.RepetitionType = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
		table.MaxRepetitionLevel = maxRepetitionLevel
		table.MaxDefinitionLevel = maxDefinitionLevel
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		j := 0
		numRows := int64(0)
		for i := 0; i < len(definitionLevels); i++ {
			dl, _ := definitionLevels[i].(int64)
			rl, _ := repetitionLevels[i].(int64)
			table.RepetitionLevels[i] = int32(rl)
			table.DefinitionLevels[i] = int32(dl)
			if table.DefinitionLevels[i] == maxDefinitionLevel {
				table.Values[i] = values[j]
				j++
			}
			if table.RepetitionLevels[i] == 0 {
				numRows++
			}
		}
		page.DataTable = table

		return page, int64(len(definitionLevels)), numRows, nil
	default:
		return nil, 0, 0, fmt.Errorf("error page type %v", pageHeader.GetType())
	}
}
