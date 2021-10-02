package parquet

import (
	"fmt"
	"reflect"

	"github.com/stdiopt/parquet/internal/parquet"
)

// GetTypes returns object type from schema by reflect.
func (sh *SchemaHandler) GetTypes() []reflect.Type {
	ln := int32(len(sh.SchemaElements))
	elements := make([][]int32, ln)
	for i := 0; i < int(ln); i++ {
		elements[i] = []int32{}
	}

	elementTypes := make([]reflect.Type, ln)

	var pos int32
	stack := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
				idx := stack[len(stack)-1][0]
				elements[idx] = append(elements[idx], pos)
			}
			item := [2]int32{pos, sh.SchemaElements[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
			continue
		}

		curlen := len(stack) - 1
		idx := stack[curlen][0]
		nc := sh.SchemaElements[idx].GetNumChildren()
		pT, cT := sh.SchemaElements[idx].Type, sh.SchemaElements[idx].ConvertedType
		rT := sh.SchemaElements[idx].RepetitionType

		if nc == 0 {
			if *rT != parquet.FieldRepetitionType_REPEATED {
				elementTypes[idx] = parquetTypeToGoReflectType(pT, rT)
			} else {
				elementTypes[idx] = reflect.SliceOf(parquetTypeToGoReflectType(pT, nil))
			}
			stack = stack[:curlen]
			continue
		}
		switch {
		case cT != nil && *cT == parquet.ConvertedType_LIST &&
			len(elements[idx]) == 1 &&
			sh.GetInName(int(elements[idx][0])) == "List" &&
			len(elements[elements[idx][0]]) == 1 &&
			sh.GetInName(int(elements[elements[idx][0]][0])) == "Element":

			cidx := elements[elements[idx][0]][0]
			elementTypes[idx] = reflect.SliceOf(elementTypes[cidx])

		case cT != nil && *cT == parquet.ConvertedType_MAP &&
			len(elements[idx]) == 1 &&
			sh.GetInName(int(elements[idx][0])) == "Key_value" &&
			len(elements[elements[idx][0]]) == 2 &&
			sh.GetInName(int(elements[elements[idx][0]][0])) == "Key" &&
			sh.GetInName(int(elements[elements[idx][0]][1])) == "Value":

			k, v := elements[elements[idx][0]][0], elements[elements[idx][0]][1]
			kT, vT := elementTypes[k], elementTypes[v]
			elementTypes[idx] = reflect.MapOf(kT, vT)

		default:
			fields := []reflect.StructField{}
			for _, ci := range elements[idx] {
				fields = append(fields, reflect.StructField{
					Name: sh.Infos[ci].InName,
					Type: elementTypes[ci],
				})
			}

			structType := reflect.StructOf(fields)

			switch {
			case rT == nil || *rT == parquet.FieldRepetitionType_REQUIRED:
				elementTypes[idx] = structType
			case *rT == parquet.FieldRepetitionType_OPTIONAL:
				elementTypes[idx] = reflect.New(structType).Type()
			case *rT == parquet.FieldRepetitionType_REPEATED:
				elementTypes[idx] = reflect.SliceOf(structType)
			}
		}

		stack = stack[:curlen]
	}

	return elementTypes
}

// GetType returns type by path, returns error if not exists.
func (sh *SchemaHandler) GetType(prefixPath string) (reflect.Type, error) {
	prefixPath, err := sh.ConvertToInPathStr(prefixPath)
	if err != nil {
		return nil, err
	}

	ts := sh.GetTypes()
	idx, ok := sh.MapIndex[prefixPath]
	if !ok {
		return nil, fmt.Errorf("[GetType] Can't find %v", prefixPath)
	}
	return ts[idx], nil
}
