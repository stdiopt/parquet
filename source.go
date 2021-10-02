package parquet

import (
	"io"

	"github.com/apache/thrift/lib/go/thrift"
)

// File to be used in parquet reader or writer.
type File interface {
	io.Seeker
	io.Reader
	io.Writer
	io.Closer
	Open(name string) (File, error)
	Create(name string) (File, error)
}

// ConvertToThriftReader convert a file reater to Thrift reader.
func ConvertToThriftReader(file File, offset int64, size int64) (*thrift.TBufferedTransport, error) {
	if _, err := file.Seek(offset, 0); err != nil {
		return nil, err
	}
	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
	return bufferReader, nil
}
