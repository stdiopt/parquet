//go:build !no_gzip
// +build !no_gzip

package compress

import (
	"bytes"
	"io/ioutil"
	"sync"

	"github.com/klauspost/compress/gzip"
	"github.com/stdiopt/parquet/internal/parquet"
)

var gzipWriterPool sync.Pool

func init() {
	gzipWriterPool = sync.Pool{
		New: func() interface{} {
			return gzip.NewWriter(nil)
		},
	}

	compressors[parquet.CompressionCodec_GZIP] = &Compressor{
		Compress: func(buf []byte) ([]byte, error) {
			res := new(bytes.Buffer)
			gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
			gzipWriter.Reset(res)
			if _, err := gzipWriter.Write(buf); err != nil {
				return nil, err
			}
			if err := gzipWriter.Close(); err != nil {
				return nil, err
			}
			gzipWriter.Reset(nil)
			gzipWriterPool.Put(gzipWriter)
			return res.Bytes(), nil
		},
		Uncompress: func(buf []byte) ([]byte, error) {
			rbuf := bytes.NewReader(buf)
			gzipReader, err := gzip.NewReader(rbuf)
			if err != nil {
				return nil, err
			}
			return ioutil.ReadAll(gzipReader)
		},
	}
}
