//go:build !no_lz4
// +build !no_lz4

package compress

import (
	"bytes"
	"io/ioutil"
	"sync"

	"github.com/pierrec/lz4/v4"
	"github.com/stdiopt/parquet/internal/parquet"
)

func init() {
	lz4WriterPool := sync.Pool{
		New: func() interface{} {
			return lz4.NewWriter(nil)
		},
	}
	compressors[parquet.CompressionCodec_LZ4] = &Compressor{
		Compress: func(buf []byte) ([]byte, error) {
			lz4Writer := lz4WriterPool.Get().(*lz4.Writer)
			res := new(bytes.Buffer)
			lz4Writer.Reset(res)
			if _, err := lz4Writer.Write(buf); err != nil {
				return nil, err
			}
			if err := lz4Writer.Close(); err != nil {
				return nil, err
			}
			lz4Writer.Reset(nil)
			lz4WriterPool.Put(lz4Writer)
			return res.Bytes(), nil
		},
		Uncompress: func(buf []byte) (i []byte, err error) {
			rbuf := bytes.NewReader(buf)
			lz4Reader := lz4.NewReader(rbuf)
			return ioutil.ReadAll(lz4Reader)
		},
	}
}
