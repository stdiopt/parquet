//go:build !no_snappy
// +build !no_snappy

package compress

import (
	"github.com/golang/snappy"
	"github.com/stdiopt/parquet/internal/parquet"
)

func init() {
	compressors[parquet.CompressionCodec_SNAPPY] = &Compressor{
		Compress: func(buf []byte) ([]byte, error) {
			return snappy.Encode(nil, buf), nil
		},
		Uncompress: func(buf []byte) ([]byte, error) {
			return snappy.Decode(nil, buf)
		},
	}
}
