package compress

import (
	"bytes"
	"testing"

	"github.com/stdiopt/parquet/internal/parquet"
)

func TestGzipCompression(t *testing.T) {
	gzipCompressor := compressors[parquet.CompressionCodec_GZIP]
	input := []byte("test data")
	compressed, err := gzipCompressor.Compress(input)
	if err != nil {
		t.Fatal(err)
	}
	output, err := gzipCompressor.Uncompress(compressed)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(input, output) {
		t.Fatalf("expected output %s but was %s", string(input), string(output))
	}
}

func BenchmarkGzipCompression(b *testing.B) {
	gzipCompressor := compressors[parquet.CompressionCodec_GZIP]
	input := []byte("test data")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := gzipCompressor.Compress(input); err != nil {
			b.Fatal(err)
		}
	}
}
