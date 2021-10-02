package compress

import (
	"errors"

	"github.com/stdiopt/parquet/internal/parquet"
)

// Compressor contains funcs to compress and decompress.
type Compressor struct {
	Compress   func(buf []byte) ([]byte, error)
	Uncompress func(buf []byte) ([]byte, error)
}

var compressors = map[parquet.CompressionCodec]*Compressor{}

// Uncompress ...
func Uncompress(buf []byte, compressMethod parquet.CompressionCodec) ([]byte, error) {
	c, ok := compressors[compressMethod]
	if !ok {
		return nil, errors.New("unsupported compress method")
	}

	return c.Uncompress(buf)
}

// Compress ...
func Compress(buf []byte, compressMethod parquet.CompressionCodec) ([]byte, error) {
	c, ok := compressors[compressMethod]
	if !ok {
		return nil, errors.New("unsupported compress method")
	}
	return c.Compress(buf)
}
