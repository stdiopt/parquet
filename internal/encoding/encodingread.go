package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/stdiopt/parquet/internal/parquet"
)

// ReadPlain ...
func ReadPlain(
	bytesReader *bytes.Reader,
	dataType parquet.Type,
	cnt uint64,
	bitWidth uint64,
) ([]interface{}, error) {
	switch dataType {
	case parquet.Type_BOOLEAN:
		return ReadPlainBOOLEAN(bytesReader, cnt)
	case parquet.Type_INT32:
		return ReadPlainINT32(bytesReader, cnt)
	case parquet.Type_INT64:
		return ReadPlainINT64(bytesReader, cnt)
	case parquet.Type_INT96:
		return ReadPlainINT96(bytesReader, cnt)
	case parquet.Type_FLOAT:
		return ReadPlainFLOAT(bytesReader, cnt)
	case parquet.Type_DOUBLE:
		return ReadPlainDOUBLE(bytesReader, cnt)
	case parquet.Type_BYTE_ARRAY:
		return ReadPlainBYTE_ARRAY(bytesReader, cnt)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, cnt, bitWidth)
	default:
		return nil, fmt.Errorf("unknown parquet type")
	}
}

// ReadPlainBOOLEAN ...
func ReadPlainBOOLEAN(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var (
		res []interface{}
		err error
	)

	res = make([]interface{}, cnt)
	resInt, err := ReadBitPacked(bytesReader, cnt<<1, 1)
	if err != nil {
		return nil, err
	}

	for i := 0; i < int(cnt); i++ {
		res[i] = false
		if resInt[i].(int64) > 0 {
			res[i] = true
		}
	}
	return res, err
}

// ReadPlainINT32 ...
func ReadPlainINT32(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadINT32(bytesReader, res)
	return res, err
}

// ReadPlainINT64 ...
func ReadPlainINT64(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadINT64(bytesReader, res)
	return res, err
}

// ReadPlainINT96 ...
func ReadPlainINT96(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	cur := make([]byte, 12)
	for i := 0; i < int(cnt); i++ {
		if _, err = bytesReader.Read(cur); err != nil {
			break
		}
		res[i] = string(cur[:12])
	}
	return res, err
}

// ReadPlainFLOAT ...
func ReadPlainFLOAT(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadFLOAT32(bytesReader, res)
	return res, err
}

// ReadPlainDOUBLE ...
func ReadPlainDOUBLE(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadFLOAT64(bytesReader, res)
	return res, err
}

// ReadPlainBYTE_ARRAY ...
// nolint: revive
func ReadPlainBYTE_ARRAY(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		buf := make([]byte, 4)
		if _, err := bytesReader.Read(buf); err != nil {
			return nil, err
		}
		ln := binary.LittleEndian.Uint32(buf)
		cur := make([]byte, ln)
		if _, err := bytesReader.Read(cur); err != nil {
			return nil, err
		}
		res[i] = string(cur)
	}
	return res, nil
}

// ReadPlainFIXED_LEN_BYTE_ARRAY
// nolint: revive
func ReadPlainFIXED_LEN_BYTE_ARRAY(
	bytesReader *bytes.Reader,
	cnt uint64,
	fixedLength uint64,
) ([]interface{}, error) {
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		cur := make([]byte, fixedLength)
		if _, err := bytesReader.Read(cur); err != nil {
			return nil, err
		}
		res[i] = string(cur)
	}
	return res, nil
}

// ReadUnsignedVarInt ...
func ReadUnsignedVarInt(bytesReader *bytes.Reader) (uint64, error) {
	var res uint64
	var shift uint64
	for {
		b, err := bytesReader.ReadByte()
		if err != nil {
			return 0, err
		}
		res |= ((uint64(b) & uint64(0x7F)) << shift)
		if (b & 0x80) == 0 {
			return 0, nil
		}
		shift += 7
	}
}

// ReadRLE return res is []INT64.
func ReadRLE(bytesReader *bytes.Reader, header uint64, bitWidth uint64) ([]interface{}, error) {
	var err error
	var res []interface{}
	cnt := header >> 1
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if width > 0 {
		if _, err = bytesReader.Read(data); err != nil {
			return res, err
		}
	}
	for len(data) < 4 {
		data = append(data, byte(0))
	}
	val := int64(binary.LittleEndian.Uint32(data))
	res = make([]interface{}, cnt)

	for i := 0; i < int(cnt); i++ {
		res[i] = val
	}
	return res, err
}

// ReadBitPacked return res is []INT64.
func ReadBitPacked(bytesReader *bytes.Reader, header, bitWidth uint64) ([]interface{}, error) {
	var err error
	numGroup := (header >> 1)
	cnt := numGroup * 8
	byteCnt := cnt * bitWidth / 8

	res := make([]interface{}, 0, cnt)

	if cnt == 0 {
		return res, nil
	}

	if bitWidth == 0 {
		for i := 0; i < int(cnt); i++ {
			res = append(res, int64(0))
		}
		return res, err
	}
	bytesBuf := make([]byte, byteCnt)
	if _, err = bytesReader.Read(bytesBuf); err != nil {
		return res, err
	}

	i := 0
	var resCur uint64
	var used uint64
	resCurNeedBits := bitWidth
	left := 8 - used
	b := bytesBuf[i]
	for i < len(bytesBuf) {
		if left >= resCurNeedBits {
			resCur |= ((uint64(b)>>used)&((1<<resCurNeedBits)-1))<<bitWidth - resCurNeedBits
			res = append(res, int64(resCur))
			left -= resCurNeedBits
			used += resCurNeedBits

			resCurNeedBits = bitWidth
			resCur = 0

			if left <= 0 && i+1 < len(bytesBuf) {
				i++
				b = bytesBuf[i]
				left = 8
				used = 0
			}
			continue
		}
		resCur |= (uint64(b)>>used)<<bitWidth - resCurNeedBits
		i++
		if i < len(bytesBuf) {
			b = bytesBuf[i]
		}
		resCurNeedBits -= left
		left = 8
		used = 0
	}
	return res, err
}

// ReadRLEBitPackedHybrid res is INT64.
func ReadRLEBitPackedHybrid(bytesReader *bytes.Reader, bitWidth uint64, length uint64) ([]interface{}, error) {
	res := make([]interface{}, 0)
	if length <= 0 {
		lb, err := ReadPlainINT32(bytesReader, 1)
		if err != nil {
			return res, err
		}
		length = uint64(lb[0].(int32))
	}

	buf := make([]byte, length)
	if _, err := bytesReader.Read(buf); err != nil {
		return res, err
	}

	newReader := bytes.NewReader(buf)
	for newReader.Len() > 0 {
		header, err := ReadUnsignedVarInt(newReader)
		if err != nil {
			return res, err
		}
		if header&1 == 0 {
			buf, err := ReadRLE(newReader, header, bitWidth)
			if err != nil {
				return nil, err
			}
			res = append(res, buf...)
			continue
		}
		buf, err := ReadBitPacked(newReader, header, bitWidth)
		if err != nil {
			return nil, err
		}
		res = append(res, buf...)
	}
	return res, nil
}

// ReadDeltaBinaryPackedINT32 ...
func ReadDeltaBinaryPackedINT32(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		err error
		res []interface{}
	)

	blockSize, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return nil, err
	}
	numMiniblocksInBlock, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return nil, err
	}
	numValues, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return nil, err
	}
	firstValueZigZag, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return nil, err
	}

	fv32 := int32(firstValueZigZag)
	firstValue := int32(uint32(fv32)>>1) ^ -(fv32 & 1)
	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]interface{}, 0)
	res = append(res, firstValue)
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return nil, err
		}

		md32 := int32(minDeltaZigZag)
		minDelta := int32(uint32(md32)>>1) ^ -(md32 & 1)
		bitWidths := make([]uint64, numMiniblocksInBlock)
		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			b, err := bytesReader.ReadByte()
			if err != nil {
				return nil, err
			}
			bitWidths[i] = uint64(b)
		}
		for i := 0; uint64(i) < numMiniblocksInBlock && uint64(len(res)) < numValues; i++ {
			cur, err := ReadBitPacked(bytesReader, (numValuesInMiniBlock/8)<<1, bitWidths[i])
			if err != nil {
				return nil, err
			}
			for j := 0; j < len(cur) && len(res) < int(numValues); j++ {
				res = append(res, res[len(res)-1].(int32)+int32(cur[j].(int64))+minDelta)
			}
		}
	}
	return res[:numValues], nil
}

// ReadDeltaBinaryPackedINT64 res is INT64.
func ReadDeltaBinaryPackedINT64(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		err error
		res []interface{}
	)

	blockSize, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return nil, err
	}
	numMiniblocksInBlock, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return nil, err
	}
	numValues, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return nil, err
	}
	firstValueZigZag, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return nil, err
	}
	firstValue := int64(firstValueZigZag>>1) ^ -(int64(firstValueZigZag) & 1)

	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]interface{}, 0)
	res = append(res, firstValue)
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, err
		}
		minDelta := int64(minDeltaZigZag>>1) ^ -(int64(minDeltaZigZag) & 1)
		bitWidths := make([]uint64, numMiniblocksInBlock)
		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			b, err := bytesReader.ReadByte()
			if err != nil {
				return nil, err
			}
			bitWidths[i] = uint64(b)
		}

		for i := 0; uint64(i) < numMiniblocksInBlock && uint64(len(res)) < numValues; i++ {
			cur, err := ReadBitPacked(bytesReader, (numValuesInMiniBlock/8)<<1, bitWidths[i])
			if err != nil {
				return nil, err
			}
			for j := 0; j < len(cur); j++ {
				res = append(res, (res[len(res)-1].(int64) + cur[j].(int64) + minDelta))
			}
		}
	}
	return res[:numValues], nil
}

// ReadDeltaLengthByteArray ...
func ReadDeltaLengthByteArray(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		res []interface{}
		err error
	)

	lengths, err := ReadDeltaBinaryPackedINT64(bytesReader)
	if err != nil {
		return res, err
	}
	res = make([]interface{}, len(lengths))
	for i := 0; i < len(lengths); i++ {
		res[i] = ""
		length := uint64(lengths[i].(int64))
		if length == 0 {
			continue
		}
		cur, err := ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, 1, length)
		if err != nil {
			return res, err
		}
		res[i] = cur[0]
	}

	return res, nil
}

// ReadDeltaByteArray ...
func ReadDeltaByteArray(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		res []interface{}
		err error
	)

	prefixLengths, err := ReadDeltaBinaryPackedINT64(bytesReader)
	if err != nil {
		return nil, err
	}
	suffixes, err := ReadDeltaLengthByteArray(bytesReader)
	if err != nil {
		return nil, err
	}
	res = make([]interface{}, len(prefixLengths))

	res[0] = suffixes[0]
	for i := 1; i < len(prefixLengths); i++ {
		prefixLength := prefixLengths[i].(int64)
		prefix := res[i-1].(string)[:prefixLength]
		suffix := suffixes[i].(string)
		res[i] = prefix + suffix
	}
	return res, nil
}

// ReadByteStreamSplitFloat32 ...
func ReadByteStreamSplitFloat32(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	res := make([]interface{}, cnt)
	buf := make([]byte, cnt*4)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return nil, err
	}
	if cnt*4 != uint64(n) {
		return nil, io.ErrUnexpectedEOF
	}

	for i := uint64(0); i < cnt; i++ {
		res[i] = math.Float32frombits(uint32(buf[i]) |
			uint32(buf[cnt+i])<<8 |
			uint32(buf[cnt*2+i])<<16 |
			uint32(buf[cnt*3+i])<<24)
	}

	return res, nil
}

// ReadByteStreamSplitFloat64 ...
func ReadByteStreamSplitFloat64(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	res := make([]interface{}, cnt)
	buf := make([]byte, cnt*8)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return nil, err
	}
	if cnt*8 != uint64(n) {
		return nil, io.ErrUnexpectedEOF
	}

	for i := uint64(0); i < cnt; i++ {
		res[i] = math.Float64frombits(uint64(buf[i]) |
			uint64(buf[cnt+i])<<8 |
			uint64(buf[cnt*2+i])<<16 |
			uint64(buf[cnt*3+i])<<24 |
			uint64(buf[cnt*4+i])<<32 |
			uint64(buf[cnt*5+i])<<40 |
			uint64(buf[cnt*6+i])<<48 |
			uint64(buf[cnt*7+i])<<56)
	}

	return res, nil
}
