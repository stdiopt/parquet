package parquet

import (
	"context"
	"encoding/binary"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stdiopt/parquet/internal/parquet"
)

// Reader contains methods and information to read parquet files.
type Reader struct {
	SchemaHandler *SchemaHandler
	NP            int64 // parallel number
	Footer        *parquet.FileMetaData
	PFile         File

	ColumnBuffers map[string]*columnBufferType

	// One reader can only read one type objects
	ObjType        reflect.Type
	ObjPartialType reflect.Type
}

// NewReader Create a parquet reader: obj is a object with schema tags or a JSON schema string.
func NewReader(pFile File, obj interface{}, np int64) (*Reader, error) {
	var err error
	res := new(Reader)
	res.NP = np
	res.PFile = pFile
	if err = res.ReadFooter(); err != nil {
		return nil, err
	}
	res.ColumnBuffers = make(map[string]*columnBufferType)

	if obj != nil {
		if sa, ok := obj.(string); ok {
			err = res.SetSchemaHandlerFromJSON(sa)
			return res, err
		} else if sa, ok := obj.([]*parquet.SchemaElement); ok {
			res.SchemaHandler = NewSchemaHandlerFromSchemaList(sa)
		} else {
			if res.SchemaHandler, err = NewSchemaHandlerFromStruct(obj); err != nil {
				return res, err
			}

			res.ObjType = reflect.TypeOf(obj).Elem()
		}
	} else {
		res.SchemaHandler = NewSchemaHandlerFromSchemaList(res.Footer.Schema)
	}

	res.RenameSchema()
	for i := 0; i < len(res.SchemaHandler.SchemaElements); i++ {
		schema := res.SchemaHandler.SchemaElements[i]
		if schema.GetNumChildren() != 0 {
			continue
		}
		pathStr := res.SchemaHandler.IndexMap[int32(i)]
		if res.ColumnBuffers[pathStr], err = newColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr); err != nil {
			return res, err
		}
	}

	return res, nil
}

// SetSchemaHandlerFromJSON reads a json schema.
func (pr *Reader) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error

	if pr.SchemaHandler, err = NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return err
	}

	pr.RenameSchema()
	for i := 0; i < len(pr.SchemaHandler.SchemaElements); i++ {
		schemaElement := pr.SchemaHandler.SchemaElements[i]
		if schemaElement.GetNumChildren() != 0 {
			continue
		}
		pathStr := pr.SchemaHandler.IndexMap[int32(i)]
		if pr.ColumnBuffers[pathStr], err = newColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr); err != nil {
			return err
		}
	}
	return nil
}

// RenameSchema renames schema name to inname.
func (pr *Reader) RenameSchema() {
	for i := 0; i < len(pr.SchemaHandler.Infos); i++ {
		pr.Footer.Schema[i].Name = pr.SchemaHandler.Infos[i].InName
	}
	for _, rowGroup := range pr.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			exPath := make([]string, 0)
			exPath = append(exPath, pr.SchemaHandler.GetRootExName())
			exPath = append(exPath, chunk.MetaData.GetPathInSchema()...)
			exPathStr := pathToStr(exPath)

			inPathStr := pr.SchemaHandler.ExPathToInPath[exPathStr]
			inPath := strToPath(inPathStr)[1:]
			chunk.MetaData.PathInSchema = inPath
		}
	}
}

// GetNumRows return number of rows.
func (pr *Reader) GetNumRows() int64 {
	return pr.Footer.GetNumRows()
}

// GetFooterSize returns the footer size.
func (pr *Reader) GetFooterSize() (uint32, error) {
	var err error
	buf := make([]byte, 4)
	if _, err = pr.PFile.Seek(-8, io.SeekEnd); err != nil {
		return 0, err
	}
	if _, err = io.ReadFull(pr.PFile, buf); err != nil {
		return 0, err
	}
	size := binary.LittleEndian.Uint32(buf)
	return size, err
}

// ReadFooter reads the footer from parquet file.
func (pr *Reader) ReadFooter() error {
	size, err := pr.GetFooterSize()
	if err != nil {
		return err
	}
	if _, err = pr.PFile.Seek(-(int64)(8+size), io.SeekEnd); err != nil {
		return err
	}
	pr.Footer = parquet.NewFileMetaData()
	sr := thrift.NewStreamTransportR(pr.PFile)
	protocol := thrift.NewTCompactProtocolConf(sr, nil)

	// pf := thrift.NewTCompactProtocolFactory()
	// protocol := pf.GetProtocol(thrift.NewStreamTransportR(pr.PFile))
	return pr.Footer.Read(context.TODO(), protocol)
}

// SkipRows of parquet file.
func (pr *Reader) SkipRows(num int64) error {
	var err error
	if num <= 0 {
		return nil
	}
	doneChan := make(chan int, pr.NP)
	taskChan := make(chan string, len(pr.SchemaHandler.ValueColumns))
	stopChan := make(chan int)

	for _, pathStr := range pr.SchemaHandler.ValueColumns {
		if _, ok := pr.ColumnBuffers[pathStr]; ok {
			continue
		}
		pr.ColumnBuffers[pathStr], err = newColumnBuffer(
			pr.PFile,
			pr.Footer,
			pr.SchemaHandler,
			pathStr,
		)
		if err != nil {
			return err
		}
	}

	for i := int64(0); i < pr.NP; i++ {
		go func() {
			for {
				select {
				case <-stopChan:
					return
				case pathStr := <-taskChan:
					cb := pr.ColumnBuffers[pathStr]
					cb.SkipRows(num)
					doneChan <- 0
				}
			}
		}()
	}

	for key := range pr.ColumnBuffers {
		taskChan <- key
	}

	for i := 0; i < len(pr.ColumnBuffers); i++ {
		<-doneChan
	}
	for i := int64(0); i < pr.NP; i++ {
		stopChan <- 0
	}
	return err
}

// Read rows of parquet file and unmarshal all to dst.
func (pr *Reader) Read(dstInterface interface{}) error {
	return pr.read(dstInterface, "")
}

// ReadByNumber read maxReadNumber objects.
func (pr *Reader) ReadByNumber(maxReadNumber int) ([]interface{}, error) {
	var err error
	if pr.ObjType == nil {
		if pr.ObjType, err = pr.SchemaHandler.GetType(pr.SchemaHandler.GetRootInName()); err != nil {
			return nil, err
		}
	}

	vs := reflect.MakeSlice(reflect.SliceOf(pr.ObjType), maxReadNumber, maxReadNumber)
	res := reflect.New(vs.Type())
	res.Elem().Set(vs)

	if err = pr.Read(res.Interface()); err != nil {
		return nil, err
	}

	ln := res.Elem().Len()
	ret := make([]interface{}, ln)
	for i := 0; i < ln; i++ {
		ret[i] = res.Elem().Index(i).Interface()
	}

	return ret, nil
}

// ReadPartial read rows of parquet file and unmarshal all to dst.
func (pr *Reader) ReadPartial(dstInterface interface{}, prefixPath string) error {
	prefixPath, err := pr.SchemaHandler.ConvertToInPathStr(prefixPath)
	if err != nil {
		return err
	}

	return pr.read(dstInterface, prefixPath)
}

// ReadPartialByNumber read maxReadNumber partial objects.
func (pr *Reader) ReadPartialByNumber(maxReadNumber int, prefixPath string) ([]interface{}, error) {
	var err error
	if pr.ObjPartialType == nil {
		if pr.ObjPartialType, err = pr.SchemaHandler.GetType(prefixPath); err != nil {
			return nil, err
		}
	}

	vs := reflect.MakeSlice(reflect.SliceOf(pr.ObjPartialType), maxReadNumber, maxReadNumber)
	res := reflect.New(vs.Type())
	res.Elem().Set(vs)

	if err = pr.ReadPartial(res.Interface(), prefixPath); err != nil {
		return nil, err
	}

	ln := res.Elem().Len()
	ret := make([]interface{}, ln)
	for i := 0; i < ln; i++ {
		ret[i] = res.Elem().Index(i).Interface()
	}

	return ret, nil
}

// read rows of parquet file with a prefixPath.
func (pr *Reader) read(dstInterface interface{}, prefixPath string) error {
	var err error
	tmap := make(map[string]*layoutTable)
	locker := new(sync.Mutex)
	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	num := reflect.ValueOf(dstInterface).Elem().Len()
	if num <= 0 {
		return nil
	}

	doneChan := make(chan int, pr.NP)
	taskChan := make(chan string, len(pr.ColumnBuffers))
	stopChan := make(chan int)

	for i := int64(0); i < pr.NP; i++ {
		go func() {
			for {
				select {
				case <-stopChan:
					return
				case pathStr := <-taskChan:
					cb := pr.ColumnBuffers[pathStr]
					table, _ := cb.ReadRows(int64(num))
					locker.Lock()
					if _, ok := tmap[pathStr]; ok {
						tmap[pathStr].Merge(table)
					} else {
						tmap[pathStr] = newLayoutTableFromTable(table)
						tmap[pathStr].Merge(table)
					}
					locker.Unlock()
					doneChan <- 0
				}
			}
		}()
	}

	readNum := 0
	for key := range pr.ColumnBuffers {
		if strings.HasPrefix(key, prefixPath) {
			taskChan <- key
			readNum++
		}
	}
	for i := 0; i < readNum; i++ {
		<-doneChan
	}

	for i := int64(0); i < pr.NP; i++ {
		stopChan <- 0
	}

	dstList := make([]interface{}, pr.NP)
	delta := (int64(num) + pr.NP - 1) / pr.NP

	var wg sync.WaitGroup
	for c := int64(0); c < pr.NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > int64(num) {
			end = int64(num)
		}
		if bgn >= int64(num) {
			bgn, end = int64(num), int64(num)
		}
		wg.Add(1)
		go func(b, e, index int) {
			defer func() {
				wg.Done()
			}()

			dstList[index] = reflect.New(reflect.SliceOf(ot)).Interface()
			if err2 := unmarshal(&tmap, b, e, dstList[index], pr.SchemaHandler, prefixPath); err2 != nil {
				err = err2
			}
		}(int(bgn), int(end), int(c))
	}

	wg.Wait()

	dstValue := reflect.ValueOf(dstInterface).Elem()
	dstValue.SetLen(0)
	for _, dst := range dstList {
		dstValue.Set(reflect.AppendSlice(dstValue, reflect.ValueOf(dst).Elem()))
	}

	return err
}

// Close closes the parquer file.
func (pr *Reader) Close() error {
	var merr error
	for _, cb := range pr.ColumnBuffers {
		if cb == nil {
			continue
		}
		if err := cb.PFile.Close(); err != nil {
			merr = err
		}
	}
	return merr
}
