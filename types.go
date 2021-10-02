package parquet

import (
	"math/big"
	"reflect"
	"strconv"

	"github.com/stdiopt/parquet/internal/parquet"
)

func parquetTypeToGoReflectType(pT *parquet.Type, rT *parquet.FieldRepetitionType) reflect.Type {
	if rT == nil || *rT != parquet.FieldRepetitionType_OPTIONAL {
		switch *pT {
		case parquet.Type_BOOLEAN:
			return reflect.TypeOf(true)
		case parquet.Type_INT32:
			return reflect.TypeOf(int32(0))
		case parquet.Type_INT64:
			return reflect.TypeOf(int64(0))
		case parquet.Type_INT96:
			return reflect.TypeOf("")
		case parquet.Type_FLOAT:
			return reflect.TypeOf(float32(0))
		case parquet.Type_DOUBLE:
			return reflect.TypeOf(float64(0))
		case parquet.Type_BYTE_ARRAY:
			return reflect.TypeOf("")
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			return reflect.TypeOf("")
		default:
			return nil
		}
	}
	switch *pT {
	case parquet.Type_BOOLEAN:
		v := true
		return reflect.TypeOf(&v)
	case parquet.Type_INT32:
		v := int32(0)
		return reflect.TypeOf(&v)
	case parquet.Type_INT64:
		v := int64(0)
		return reflect.TypeOf(&v)
	case parquet.Type_INT96:
		v := ""
		return reflect.TypeOf(&v)
	case parquet.Type_FLOAT:
		v := float32(0)
		return reflect.TypeOf(&v)
	case parquet.Type_DOUBLE:
		v := float64(0)
		return reflect.TypeOf(&v)
	case parquet.Type_BYTE_ARRAY:
		v := ""
		return reflect.TypeOf(&v)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		v := ""
		return reflect.TypeOf(&v)
	default:
		return nil
	}
}

// Scan a string to parquet value; length and scale just for decimal.
func strToParquetType(
	s string,
	pT *parquet.Type,
	cT *parquet.ConvertedType,
	length int,
	scale int,
) (interface{}, error) {
	if cT == nil {
		switch *pT {
		case parquet.Type_BOOLEAN:
			return strconv.ParseBool(s)
		case parquet.Type_INT32:
			v, err := strconv.ParseInt(s, 10, 32)
			return int32(v), err
		case parquet.Type_INT64:
			return strconv.ParseInt(s, 10, 64)
		case parquet.Type_INT96:
			return strIntToBinary(s, "LittleEndian", 12, true)
		case parquet.Type_FLOAT:
			f, err := strconv.ParseFloat(s, 32)
			return float32(f), err
		case parquet.Type_DOUBLE:
			return strconv.ParseFloat(s, 64)
		case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			return s, nil
		}
		return nil, nil
	}

	switch *cT {
	case parquet.ConvertedType_UTF8:
		return s, nil
	case parquet.ConvertedType_INT_8:
		v, err := strconv.ParseInt(s, 10, 8)
		return int32(v), err
	case parquet.ConvertedType_INT_16:
		v, err := strconv.ParseInt(s, 10, 16)
		return int32(v), err

	case parquet.ConvertedType_INT_32:
		v, err := strconv.ParseInt(s, 10, 32)
		return int32(v), err
	case parquet.ConvertedType_UINT_8:
		v, err := strconv.ParseUint(s, 10, 8)
		return int32(v), err

	case parquet.ConvertedType_UINT_16:
		v, err := strconv.ParseUint(s, 10, 16)
		return int32(v), err
	case parquet.ConvertedType_UINT_32:
		v, err := strconv.ParseUint(s, 10, 32)
		return int32(v), err
	case parquet.ConvertedType_DATE, parquet.ConvertedType_TIME_MILLIS:
		v, err := strconv.ParseInt(s, 10, 32)
		return int32(v), err

	case parquet.ConvertedType_UINT_64:
		v, err := strconv.ParseUint(s, 10, 64)
		return int64(v), err

	case parquet.ConvertedType_INT_64, parquet.ConvertedType_TIME_MICROS,
		parquet.ConvertedType_TIMESTAMP_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS:
		return strconv.ParseInt(s, 10, 64)

	case parquet.ConvertedType_INTERVAL:
		return strIntToBinary(s, "LittleEndian", 12, false)
	case parquet.ConvertedType_DECIMAL:
		numSca := big.NewFloat(1.0)
		for i := 0; i < scale; i++ {
			numSca.Mul(numSca, big.NewFloat(10))
		}
		num := new(big.Float)
		num.SetString(s)
		num.Mul(num, numSca)

		switch *pT {
		case parquet.Type_INT32:
			tmp, _ := num.Float64()
			return int32(tmp), nil
		case parquet.Type_INT64:
			tmp, _ := num.Float64()
			return int64(tmp), nil
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			s = num.Text('f', 0)
			return strIntToBinary(s, "BigEndian", length, true)
		default:
			s = num.Text('f', 0)
			return strIntToBinary(s, "BigEndian", 0, true)
		}
	default:
		return nil, nil // error here?
	}
}

func interfaceToParquetType(src interface{}, pT *parquet.Type) interface{} {
	if src == nil {
		return src
	}

	if pT == nil {
		return src
	}

	switch *pT {
	case parquet.Type_BOOLEAN:
		if _, ok := src.(bool); ok {
			return src
		}
		return reflect.ValueOf(src).Bool()
	case parquet.Type_INT32:
		if _, ok := src.(int32); ok {
			return src
		}
		return int32(reflect.ValueOf(src).Int())
	case parquet.Type_INT64:
		if _, ok := src.(int64); ok {
			return src
		}
		return reflect.ValueOf(src).Int()
	case parquet.Type_FLOAT:
		if _, ok := src.(float32); ok {
			return src
		}
		return float32(reflect.ValueOf(src).Float())
	case parquet.Type_DOUBLE:
		if _, ok := src.(float64); ok {
			return src
		}
		return reflect.ValueOf(src).Float()
	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if _, ok := src.(string); ok {
			return src
		}
		return reflect.ValueOf(src).String()
	default:
		return src
	}
}

// order=LittleEndian or BigEndian; length is byte num.
func strIntToBinary(num string, order string, length int, signed bool) (string, error) {
	bigNum := new(big.Int)
	bigNum.SetString(num, 10)
	if !signed {
		res := bigNum.Bytes()
		if len(res) < length {
			res = append(make([]byte, length-len(res)), res...)
		}
		if order == "LittleEndian" {
			for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
				res[i], res[j] = res[j], res[i]
			}
		}
		if length > 0 {
			res = res[len(res)-length:]
		}
		return string(res), nil
	}

	flag := bigNum.Cmp(big.NewInt(0))
	if flag == 0 {
		if length <= 0 {
			length = 1
		}
		return string(make([]byte, length)), nil
	}

	bigNum = bigNum.SetBytes(bigNum.Bytes())
	bs := bigNum.Bytes()

	if len(bs) < length {
		bs = append(make([]byte, length-len(bs)), bs...)
	}

	upperBs := make([]byte, len(bs))
	upperBs[0] = byte(0x80)
	upper := new(big.Int)
	upper.SetBytes(upperBs)
	if flag > 0 {
		upper = upper.Sub(upper, big.NewInt(1))
	}

	if bigNum.Cmp(upper) > 0 {
		bs = append(make([]byte, 1), bs...)
	}

	if flag < 0 {
		modBs := make([]byte, len(bs)+1)
		modBs[0] = byte(0x01)
		mod := new(big.Int)
		mod.SetBytes(modBs)
		bs = mod.Sub(mod, bigNum).Bytes()
	}
	if length > 0 {
		bs = bs[len(bs)-length:]
	}
	if order == "LittleEndian" {
		for i, j := 0, len(bs)-1; i < j; i, j = i+1, j-1 {
			bs[i], bs[j] = bs[j], bs[i]
		}
	}
	return string(bs), nil
}

/*func jsonTypeToParquetType(
	val reflect.Value,
	pT *parquet.Type,
	cT *parquet.ConvertedType,
	length int,
	scale int,
) interface{} {
	if val.Type().Kind() == reflect.Interface && val.IsNil() {
		return nil
	}
	s := fmt.Sprintf("%v", val)
	return strToParquetType(s, pT, cT, length, scale)
}*/
