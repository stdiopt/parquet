package parquet

//nolint: revive

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/stdiopt/parquet/internal/parquet"
)

// Tag parser
// `parquet:"name=Name, type=FIXED_LEN_BYTE_ARRAY, length=12"`.
type Tag struct {
	InName string
	ExName string

	Type      string
	KeyType   string
	ValueType string

	ConvertedType      string
	KeyConvertedType   string
	ValueConvertedType string

	Length      int32
	KeyLength   int32
	ValueLength int32

	Scale      int32
	KeyScale   int32
	ValueScale int32

	Precision      int32
	KeyPrecision   int32
	ValuePrecision int32

	IsAdjustedToUTC      bool
	KeyIsAdjustedToUTC   bool
	ValueIsAdjustedToUTC bool

	FieldID      int32
	KeyFieldID   int32
	ValueFieldID int32

	Encoding      parquet.Encoding
	KeyEncoding   parquet.Encoding
	ValueEncoding parquet.Encoding

	OmitStats      bool
	KeyOmitStats   bool
	ValueOmitStats bool

	RepetitionType      parquet.FieldRepetitionType
	KeyRepetitionType   parquet.FieldRepetitionType
	ValueRepetitionType parquet.FieldRepetitionType

	LogicalTypeFields      map[string]string
	KeyLogicalTypeFields   map[string]string
	ValueLogicalTypeFields map[string]string
}

// NewTag returns an initialized tag.
func NewTag() *Tag {
	return &Tag{
		LogicalTypeFields:      make(map[string]string),
		KeyLogicalTypeFields:   make(map[string]string),
		ValueLogicalTypeFields: make(map[string]string),
	}
}

// StringToTag parses tag.
func StringToTag(tag string) (*Tag, error) {
	mp := NewTag()
	tagStr := strings.ReplaceAll(tag, "\t", "")
	tags := strings.Split(tagStr, ",")

	for _, tag := range tags {
		tag = strings.TrimSpace(tag)

		kv := strings.SplitN(tag, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("expect 'key=value' but got '%s'", tag)
		}
		key := kv[0]
		key = strings.ToLower(key)
		key = strings.TrimSpace(key)

		val := kv[1]
		val = strings.TrimSpace(val)

		var err error
		switch key {
		case "type":
			mp.Type = val
		case "keytype":
			mp.KeyType = val
		case "valuetype":
			mp.ValueType = val
		case "convertedtype":
			mp.ConvertedType = val
		case "keyconvertedtype":
			mp.KeyConvertedType = val
		case "valueconvertedtype":
			mp.ValueConvertedType = val
		case "length":
			if mp.Length, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse length: %w", err)
			}
		case "keylength":
			if mp.KeyLength, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse keylength: %w", err)
			}
		case "valuelength":
			if mp.ValueLength, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse valuelength: %w", err)
			}
		case "scale":
			if mp.Scale, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse scale: %w", err)
			}
		case "keyscale":
			if mp.KeyScale, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyscale: %w", err)
			}
		case "valuescale":
			if mp.ValueScale, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse valuescale: %w", err)
			}
		case "precision":
			if mp.Precision, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse precision: %w", err)
			}
		case "keyprecision":
			if mp.KeyPrecision, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyprecision: %w", err)
			}
		case "valueprecision":
			if mp.ValuePrecision, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse valueprecision: %w", err)
			}
		case "fieldid":
			if mp.FieldID, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse fieldid: %w", err)
			}
		case "keyfieldid":
			if mp.KeyFieldID, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyfieldid: %w", err)
			}
		case "valuefieldid":
			if mp.ValueFieldID, err = str2Int32(val); err != nil {
				return nil, fmt.Errorf("failed to parse valuefieldid: %w", err)
			}
		case "isadjustedtoutc":
			if mp.IsAdjustedToUTC, err = str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse isadjustedtoutc: %w", err)
			}
		case "keyisadjustedtoutc":
			if mp.KeyIsAdjustedToUTC, err = str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyisadjustedtoutc: %w", err)
			}
		case "valueisadjustedtoutc":
			if mp.ValueIsAdjustedToUTC, err = str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse valueisadjustedtoutc: %w", err)
			}
		case "name":
			if mp.InName == "" {
				mp.InName = StringToVariableName(val)
			}
			mp.ExName = val
		case "inname":
			mp.InName = val
		case "omitstats":
			if mp.OmitStats, err = str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse omitstats: %w", err)
			}
		case "keyomitstats":
			if mp.KeyOmitStats, err = str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse keyomitstats: %w", err)
			}
		case "valueomitstats":
			if mp.ValueOmitStats, err = str2Bool(val); err != nil {
				return nil, fmt.Errorf("failed to parse valueomitstats: %w", err)
			}
		case "repetitiontype":
			switch strings.ToLower(val) {
			case "repeated":
				mp.RepetitionType = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
			default:
				return nil, fmt.Errorf("unknown repetitiontype: '%v'", val)
			}
		case "keyrepetitiontype":
			switch strings.ToLower(val) {
			case "repeated":
				mp.KeyRepetitionType = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp.KeyRepetitionType = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp.KeyRepetitionType = parquet.FieldRepetitionType_OPTIONAL
			default:
				return nil, fmt.Errorf("unknown keyrepetitiontype: '%v'", val)
			}
		case "valuerepetitiontype":
			switch strings.ToLower(val) {
			case "repeated":
				mp.ValueRepetitionType = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp.ValueRepetitionType = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp.ValueRepetitionType = parquet.FieldRepetitionType_OPTIONAL
			default:
				return nil, fmt.Errorf("unknown valuerepetitiontype: '%v'", val)
			}
		case "encoding":
			switch strings.ToLower(val) {
			case "plain":
				mp.Encoding = parquet.Encoding_PLAIN
			case "rle":
				mp.Encoding = parquet.Encoding_RLE
			case "delta_binary_packed":
				mp.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
			case "delta_length_byte_array":
				mp.Encoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			case "delta_byte_array":
				mp.Encoding = parquet.Encoding_DELTA_BYTE_ARRAY
			case "plain_dictionary":
				mp.Encoding = parquet.Encoding_PLAIN_DICTIONARY
			case "rle_dictionary":
				mp.Encoding = parquet.Encoding_RLE_DICTIONARY
			case "byte_stream_split":
				mp.Encoding = parquet.Encoding_BYTE_STREAM_SPLIT
			default:
				return nil, fmt.Errorf("unknown encoding type: '%v'", val)
			}
		case "keyencoding":
			switch strings.ToLower(val) {
			case "rle":
				mp.KeyEncoding = parquet.Encoding_RLE
			case "delta_binary_packed":
				mp.KeyEncoding = parquet.Encoding_DELTA_BINARY_PACKED
			case "delta_length_byte_array":
				mp.KeyEncoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			case "delta_byte_array":
				mp.KeyEncoding = parquet.Encoding_DELTA_BYTE_ARRAY
			case "plain_dictionary":
				mp.KeyEncoding = parquet.Encoding_PLAIN_DICTIONARY
			case "byte_stream_split":
				mp.KeyEncoding = parquet.Encoding_BYTE_STREAM_SPLIT
			default:
				return nil, fmt.Errorf("unknown keyencoding type: '%v'", val)
			}
		case "valueencoding":
			switch strings.ToLower(val) {
			case "rle":
				mp.ValueEncoding = parquet.Encoding_RLE
			case "delta_binary_packed":
				mp.ValueEncoding = parquet.Encoding_DELTA_BINARY_PACKED
			case "delta_length_byte_array":
				mp.ValueEncoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			case "delta_byte_array":
				mp.ValueEncoding = parquet.Encoding_DELTA_BYTE_ARRAY
			case "plain_dictionary":
				mp.ValueEncoding = parquet.Encoding_PLAIN_DICTIONARY
			case "byte_stream_split":
				mp.ValueEncoding = parquet.Encoding_BYTE_STREAM_SPLIT
			default:
				return nil, fmt.Errorf("unknown valueencoding type: '%v'", val)
			}
		default:
			switch {
			case strings.HasPrefix(key, "logicaltype"):
				mp.LogicalTypeFields[key] = val
			case strings.HasPrefix(key, "keylogicaltype"):
				newKey := key[3:]
				mp.KeyLogicalTypeFields[newKey] = val
			case strings.HasPrefix(key, "valuelogicaltype"):
				newKey := key[5:]
				mp.ValueLogicalTypeFields[newKey] = val
			default:
				return nil, fmt.Errorf("unrecognized tag '%v'", key)
			}
		}
	}
	return mp, nil
}

// NewSchemaElementFromTagMap returns a schema element from a tag struct.
func NewSchemaElementFromTagMap(info *Tag) (*parquet.SchemaElement, error) {
	schema := parquet.NewSchemaElement()
	schema.Name = info.InName
	schema.TypeLength = &info.Length
	schema.Scale = &info.Scale
	schema.Precision = &info.Precision
	schema.FieldID = &info.FieldID
	schema.RepetitionType = &info.RepetitionType
	schema.NumChildren = nil

	if t, err := parquet.TypeFromString(info.Type); err == nil {
		schema.Type = &t
	} else {
		return nil, fmt.Errorf("type " + info.Type + ": " + err.Error())
	}

	if ct, err := parquet.ConvertedTypeFromString(info.ConvertedType); err == nil {
		schema.ConvertedType = &ct
	}

	var logicalType *parquet.LogicalType
	var err error
	if len(info.LogicalTypeFields) > 0 {
		logicalType, err = NewLogicalTypeFromFieldsMap(info.LogicalTypeFields)
		if err != nil {
			return nil, fmt.Errorf("failed to create logicaltype from field map: %w", err)
		}
	} else {
		logicalType = NewLogicalTypeFromConvertedType(schema, info)
	}

	schema.LogicalType = logicalType

	return schema, nil
}

// NewLogicalTypeFromFieldsMap returns a logicalType from a map.
func NewLogicalTypeFromFieldsMap(mp map[string]string) (*parquet.LogicalType, error) {
	val, ok := mp["logicaltype"]
	if !ok {
		return nil, errors.New("does not have logicaltype")
	}
	var err error
	logicalType := parquet.NewLogicalType()
	switch val {
	case "STRING":
		logicalType.STRING = parquet.NewStringType()
	case "MAP":
		logicalType.MAP = parquet.NewMapType()
	case "LIST":
		logicalType.LIST = parquet.NewListType()
	case "ENUM":
		logicalType.ENUM = parquet.NewEnumType()

	case "DECIMAL":
		logicalType.DECIMAL = parquet.NewDecimalType()
		logicalType.DECIMAL.Precision, err = str2Int32(mp["logicaltype.precision"])
		if err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.precision as int32: %w", err)
		}
		logicalType.DECIMAL.Scale, err = str2Int32(mp["logicaltype.scale"])
		if err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.scale as int32: %w", err)
		}

	case "DATE":
		logicalType.DATE = parquet.NewDateType()

	case "TIME":
		logicalType.TIME = parquet.NewTimeType()
		logicalType.TIME.IsAdjustedToUTC, err = str2Bool(mp["logicaltype.isadjustedtoutc"])
		if err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.isadjustedtoutc as boolean: %w", err)
		}
		switch mp["logicaltype.unit"] {
		case "MILLIS":
			logicalType.TIME.Unit = parquet.NewTimeUnit()
			logicalType.TIME.Unit.MILLIS = parquet.NewMilliSeconds()
		case "MICROS":
			logicalType.TIME.Unit = parquet.NewTimeUnit()
			logicalType.TIME.Unit.MICROS = parquet.NewMicroSeconds()
		case "NANOS":
			logicalType.TIME.Unit = parquet.NewTimeUnit()
			logicalType.TIME.Unit.NANOS = parquet.NewNanoSeconds()
		default:
			return nil, fmt.Errorf("logicaltype time error, unknown unit: %s", mp["logicaltype.unit"])
		}

	case "TIMESTAMP":
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		logicalType.TIMESTAMP.IsAdjustedToUTC, err = str2Bool(mp["logicaltype.isadjustedtoutc"])
		if err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.isadjustedtoutc as boolean: %w", err)
		}
		switch mp["logicaltype.unit"] {
		case "MILLIS":
			logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
			logicalType.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()
		case "MICROS":
			logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
			logicalType.TIMESTAMP.Unit.MICROS = parquet.NewMicroSeconds()
		case "NANOS":
			logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
			logicalType.TIMESTAMP.Unit.NANOS = parquet.NewNanoSeconds()
		default:
			return nil, fmt.Errorf("logicaltype time error, unknown unit: %s", mp["logicaltype.unit"])
		}

	case "INTEGER":
		logicalType.INTEGER = parquet.NewIntType()
		bitWidth, err := str2Int32(mp["logicaltype.bitwidth"])
		if err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.bitwidth as int32: %w", err)
		}
		logicalType.INTEGER.BitWidth = int8(bitWidth)
		logicalType.INTEGER.IsSigned, err = str2Bool(mp["logicaltype.issigned"])
		if err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.issigned as boolean: %w", err)
		}

	case "JSON":
		logicalType.JSON = parquet.NewJsonType()

	case "BSON":
		logicalType.BSON = parquet.NewBsonType()

	case "UUID":
		logicalType.UUID = parquet.NewUUIDType()

	default:
		return nil, fmt.Errorf("unknow logicaltype: " + val)
	}

	return logicalType, nil
}

// NewLogicalTypeFromConvertedType returns a parquet logical type from schema element and tag.
func NewLogicalTypeFromConvertedType(schemaElement *parquet.SchemaElement, info *Tag) *parquet.LogicalType {
	_, ct := schemaElement.Type, schemaElement.ConvertedType
	if ct == nil {
		return nil
	}

	logicalType := parquet.NewLogicalType()
	switch *ct {
	case parquet.ConvertedType_INT_8:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 8
		logicalType.INTEGER.IsSigned = true
	case parquet.ConvertedType_INT_16:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 16
		logicalType.INTEGER.IsSigned = true
	case parquet.ConvertedType_INT_32:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 32
		logicalType.INTEGER.IsSigned = true
	case parquet.ConvertedType_INT_64:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 64
		logicalType.INTEGER.IsSigned = true
	case parquet.ConvertedType_UINT_8:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 8
		logicalType.INTEGER.IsSigned = false
	case parquet.ConvertedType_UINT_16:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 16
		logicalType.INTEGER.IsSigned = false
	case parquet.ConvertedType_UINT_32:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 32
		logicalType.INTEGER.IsSigned = false
	case parquet.ConvertedType_UINT_64:
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = 64
		logicalType.INTEGER.IsSigned = false

	case parquet.ConvertedType_DECIMAL:
		logicalType.DECIMAL = parquet.NewDecimalType()
		logicalType.DECIMAL.Precision = info.Precision
		logicalType.DECIMAL.Scale = info.Scale

	case parquet.ConvertedType_DATE:
		logicalType.DATE = parquet.NewDateType()

	case parquet.ConvertedType_TIME_MICROS:
		logicalType.TIME = parquet.NewTimeType()
		logicalType.TIME.IsAdjustedToUTC = info.IsAdjustedToUTC
		logicalType.TIME.Unit = parquet.NewTimeUnit()
		logicalType.TIME.Unit.MICROS = parquet.NewMicroSeconds()

	case parquet.ConvertedType_TIME_MILLIS:
		logicalType.TIME = parquet.NewTimeType()
		logicalType.TIME.IsAdjustedToUTC = info.IsAdjustedToUTC
		logicalType.TIME.Unit = parquet.NewTimeUnit()
		logicalType.TIME.Unit.MILLIS = parquet.NewMilliSeconds()

	case parquet.ConvertedType_TIMESTAMP_MICROS:
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		logicalType.TIMESTAMP.IsAdjustedToUTC = info.IsAdjustedToUTC
		logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
		logicalType.TIMESTAMP.Unit.MICROS = parquet.NewMicroSeconds()

	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		logicalType.TIMESTAMP.IsAdjustedToUTC = info.IsAdjustedToUTC
		logicalType.TIMESTAMP.Unit = parquet.NewTimeUnit()
		logicalType.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()

	case parquet.ConvertedType_BSON:
		logicalType.BSON = parquet.NewBsonType()

	case parquet.ConvertedType_ENUM:
		logicalType.ENUM = parquet.NewEnumType()

	case parquet.ConvertedType_JSON:
		logicalType.JSON = parquet.NewJsonType()

	case parquet.ConvertedType_LIST:
		logicalType.LIST = parquet.NewListType()

	case parquet.ConvertedType_MAP:
		logicalType.MAP = parquet.NewMapType()

	case parquet.ConvertedType_UTF8:
		logicalType.STRING = parquet.NewStringType()
	default:
		return nil
	}

	return logicalType
}

// DeepCopy a weird no no
// nolint errcheck
//
// TODO: {lpf} aparently this is only used to copy a Tag structure
// which would be just better to create a specific tag clone method
func DeepCopy(src, dst interface{}) {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(src)
	gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

// GetKeyTagMap gets key tag map for map.
func GetKeyTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Key"
	res.ExName = "key"
	res.Type = src.KeyType
	res.ConvertedType = src.KeyConvertedType
	res.IsAdjustedToUTC = src.KeyIsAdjustedToUTC
	res.Length = src.KeyLength
	res.Scale = src.KeyScale
	res.Precision = src.KeyPrecision
	res.FieldID = src.KeyFieldID
	res.Encoding = src.KeyEncoding
	res.OmitStats = src.KeyOmitStats
	res.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	return res
}

// GetValueTagMap gets value tag map for map.
func GetValueTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Value"
	res.ExName = "value"
	res.Type = src.ValueType
	res.ConvertedType = src.ValueConvertedType
	res.IsAdjustedToUTC = src.ValueIsAdjustedToUTC
	res.Length = src.ValueLength
	res.Scale = src.ValueScale
	res.Precision = src.ValuePrecision
	res.FieldID = src.ValueFieldID
	res.Encoding = src.ValueEncoding
	res.OmitStats = src.ValueOmitStats
	res.RepetitionType = src.ValueRepetitionType
	return res
}

// StringToVariableName convert string to a golang variable name.
func StringToVariableName(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	name := ""
	for i := 0; i < ln; i++ {
		c := str[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			name += string(c)
		} else {
			name += strconv.Itoa(int(c))
		}
	}

	name = HeadToUpper(name)
	return name
}

// HeadToUpper convert the first letter of a string to uppercase.
func HeadToUpper(str string) string {
	if len(str) == 0 {
		return str
	}

	c := str[0]
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return strings.ToUpper(str[0:1]) + str[1:]
	}
	// handle non-alpha prefix such as "_"
	return "PARGO_PREFIX_" + str
}

func cmpIntBinary(as string, bs string, order string, signed bool) bool {
	abs, bbs := []byte(as), []byte(bs)
	la, lb := len(abs), len(bbs)

	if order == "LittleEndian" {
		for i, j := 0, len(abs)-1; i < j; i, j = i+1, j-1 {
			abs[i], abs[j] = abs[j], abs[i]
		}
		for i, j := 0, len(bbs)-1; i < j; i, j = i+1, j-1 {
			bbs[i], bbs[j] = bbs[j], bbs[i]
		}
	}
	if !signed {
		if la < lb {
			abs = append(make([]byte, lb-la), abs...)
		} else if lb < la {
			bbs = append(make([]byte, la-lb), bbs...)
		}
	} else {
		if la < lb {
			sb := (abs[0] >> 7) & 1
			pre := make([]byte, lb-la)
			if sb == 1 {
				for i := 0; i < lb-la; i++ {
					pre[i] = byte(0xFF)
				}
			}
			abs = append(pre, abs...)
		} else if la > lb {
			sb := (bbs[0] >> 7) & 1
			pre := make([]byte, la-lb)
			if sb == 1 {
				for i := 0; i < la-lb; i++ {
					pre[i] = byte(0xFF)
				}
			}
			bbs = append(pre, bbs...)
		}

		asb, bsb := (abs[0]>>7)&1, (bbs[0]>>7)&1

		if asb < bsb {
			return false
		} else if asb > bsb {
			return true
		}
	}

	for i := 0; i < len(abs); i++ {
		if abs[i] < bbs[i] {
			return true
		} else if abs[i] > bbs[i] {
			return false
		}
	}
	return false
}

func findFuncTable(pT *parquet.Type, cT *parquet.ConvertedType, logT *parquet.LogicalType) funcTable {
	if cT == nil && logT == nil {
		switch *pT {
		case parquet.Type_BOOLEAN:
			return boolFuncTable{}
		case parquet.Type_INT32:
			return int32FuncTable{}
		case parquet.Type_INT64:
			return int64FuncTable{}
		case parquet.Type_INT96:
			return int96FuncTable{}
		case parquet.Type_FLOAT:
			return float32FuncTable{}
		case parquet.Type_DOUBLE:
			return float64FuncTable{}
		case parquet.Type_BYTE_ARRAY:
			return stringFuncTable{}
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			return stringFuncTable{}
		}
	}

	if cT != nil {
		switch *cT {
		case parquet.ConvertedType_UTF8, parquet.ConvertedType_BSON, parquet.ConvertedType_JSON:
			return stringFuncTable{}

		case parquet.ConvertedType_INT_8, parquet.ConvertedType_INT_16, parquet.ConvertedType_INT_32,
			parquet.ConvertedType_DATE, parquet.ConvertedType_TIME_MILLIS:
			return int32FuncTable{}
		case parquet.ConvertedType_UINT_8, parquet.ConvertedType_UINT_16, parquet.ConvertedType_UINT_32:
			return uint32FuncTable{}

		case parquet.ConvertedType_INT_64, parquet.ConvertedType_TIME_MICROS,
			parquet.ConvertedType_TIMESTAMP_MILLIS, parquet.ConvertedType_TIMESTAMP_MICROS:
			return int64FuncTable{}
		case parquet.ConvertedType_UINT_64:
			return uint64FuncTable{}
		case parquet.ConvertedType_INTERVAL:
			return intervalFuncTable{}
		case parquet.ConvertedType_DECIMAL:
			switch *pT {
			case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
				return decimalStringFuncTable{}
			case parquet.Type_INT32:
				return int32FuncTable{}
			case parquet.Type_INT64:
				return int64FuncTable{}
			}
		}
	}

	if logT != nil {
		switch {
		case logT.TIME != nil || logT.TIMESTAMP != nil:
			return findFuncTable(pT, nil, nil)
		case logT.DATE != nil:
			return int32FuncTable{}
		case logT.INTEGER != nil:
			if logT.INTEGER.IsSigned {
				return findFuncTable(pT, nil, nil)
			}
			switch *pT {
			case parquet.Type_INT32:
				return uint32FuncTable{}
			case parquet.Type_INT64:
				return uint64FuncTable{}
			}
		case logT.DECIMAL != nil:
			switch *pT {
			case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
				return decimalStringFuncTable{}
			case parquet.Type_INT32:
				return int32FuncTable{}
			case parquet.Type_INT64:
				return int64FuncTable{}
			}
		case logT.BSON != nil || logT.JSON != nil || logT.STRING != nil || logT.UUID != nil:
			return stringFuncTable{}
		}
	}

	panic("No known func table in FindFuncTable")
}

func str2Int32(val string) (int32, error) {
	valInt, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("str2Int32: %w", err)
	}
	return int32(valInt), nil
}

func str2Bool(val string) (bool, error) {
	valBoolean, err := strconv.ParseBool(val)
	if err != nil {
		return false, fmt.Errorf("str2Bool: %w", err)
	}
	return valBoolean, nil
}

type funcTable interface {
	LessThan(a interface{}, b interface{}) bool
	MinMaxSize(minVal interface{}, maxVal interface{}, val interface{}) (interface{}, interface{}, int32)
}

func min(table funcTable, a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return a
	}
	return b
}

func max(table funcTable, a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return b
	}
	return a
}

type boolFuncTable struct{}

func (boolFuncTable) LessThan(a interface{}, b interface{}) bool {
	return !a.(bool) && b.(bool)
}

func (table boolFuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val), max(table, maxVal, val), 1
}

type int32FuncTable struct{}

func (int32FuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(int32) < b.(int32)
}

func (table int32FuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val), max(table, maxVal, val), 4
}

type uint32FuncTable struct{}

func (uint32FuncTable) LessThan(a interface{}, b interface{}) bool {
	return uint32(a.(int32)) < uint32(b.(int32))
}

func (table uint32FuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val), max(table, maxVal, val), 4
}

type int64FuncTable struct{}

func (int64FuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(int64) < b.(int64)
}

func (table int64FuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val), max(table, maxVal, val), 8
}

type uint64FuncTable struct{}

func (uint64FuncTable) LessThan(a interface{}, b interface{}) bool {
	return uint64(a.(int64)) < uint64(b.(int64))
}

func (table uint64FuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val), max(table, maxVal, val), 8
}

type int96FuncTable struct{}

func (int96FuncTable) LessThan(ai interface{}, bi interface{}) bool {
	a, b := []byte(ai.(string)), []byte(bi.(string))
	fa, fb := a[11]>>7, b[11]>>7
	if fa > fb {
		return true
	} else if fa < fb {
		return false
	}
	for i := 11; i >= 0; i-- {
		if a[i] < b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}
	return false
}

func (table int96FuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val), max(table, maxVal, val), int32(len(val.(string)))
}

type float32FuncTable struct{}

func (float32FuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(float32) < b.(float32)
}

func (table float32FuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val), max(table, maxVal, val), 4
}

type float64FuncTable struct{}

func (float64FuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(float64) < b.(float64)
}

func (table float64FuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val), max(table, maxVal, val), 8
}

type stringFuncTable struct{}

func (stringFuncTable) LessThan(a interface{}, b interface{}) bool {
	return a.(string) < b.(string)
}

func (table stringFuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val), max(table, maxVal, val), int32(len(val.(string)))
}

type intervalFuncTable struct{}

func (intervalFuncTable) LessThan(ai interface{}, bi interface{}) bool {
	a, b := []byte(ai.(string)), []byte(bi.(string))
	for i := 11; i >= 0; i-- {
		if a[i] > b[i] {
			return false
		} else if a[i] < b[i] {
			return true
		}
	}
	return false
}

func (table intervalFuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val),
		max(table, maxVal, val),
		int32(len(val.(string)))
}

type decimalStringFuncTable struct{}

func (decimalStringFuncTable) LessThan(a interface{}, b interface{}) bool {
	return cmpIntBinary(a.(string), b.(string), "BigEndian", true)
}

func (table decimalStringFuncTable) MinMaxSize(
	minVal interface{},
	maxVal interface{},
	val interface{},
) (interface{}, interface{}, int32) {
	return min(table, minVal, val),
		max(table, maxVal, val),
		int32(len(val.(string)))
}

// SizeOf Get the size of a parquet value.
func SizeOf(val reflect.Value) int64 {
	var size int64
	switch val.Type().Kind() {
	case reflect.Ptr:
		if val.IsNil() {
			return 0
		}
		return SizeOf(val.Elem())
	case reflect.Slice:
		for i := 0; i < val.Len(); i++ {
			size += SizeOf(val.Index(i))
		}
		return size
	case reflect.Struct:
		for i := 0; i < val.Type().NumField(); i++ {
			size += SizeOf(val.Field(i))
		}
		return size
	case reflect.Map:
		keys := val.MapKeys()
		for i := 0; i < len(keys); i++ {
			size += SizeOf(keys[i])
			size += SizeOf(val.MapIndex(keys[i]))
		}
		return size
	case reflect.Bool:
		return 1
	case reflect.Int32:
		return 4
	case reflect.Int64:
		return 8
	case reflect.String:
		return int64(val.Len())
	case reflect.Float32:
		return 4
	case reflect.Float64:
		return 8
	}
	return 4
}

const parGOPathDelimiter = "\x01"

// . -> \x01
// func reformPathStr(pathStr string) string {
//	return strings.ReplaceAll(pathStr, ".", "\x01")
// }

// Convert path slice to string.
func pathToStr(path []string) string {
	return strings.Join(path, parGOPathDelimiter)
}

// Convert string to path slice.
func strToPath(str string) []string {
	return strings.Split(str, parGOPathDelimiter)
}

// Get the pathStr index in a path.
func pathStrIndex(str string) int {
	return len(strings.Split(str, parGOPathDelimiter))
}
