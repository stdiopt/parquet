package parquet

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/stdiopt/parquet/internal/parquet"
)

/*
PathMap Example
            root(a dummy root)  (Path: "root", Children: A)
             |
             A  (Path:"root/A", Childend: B,C)
        /           \
B(Path:"root/A/B")   C(Path:"root/A/C")
*/

// PathMapType records the path and its children; This is used in Marshal for improve performance.
type PathMapType struct {
	Path     string
	Children map[string]*PathMapType
}

// NewPathMap returns am initialized pathMap type.
func NewPathMap(path string) *PathMapType {
	pathMap := new(PathMapType)
	pathMap.Path = path
	pathMap.Children = make(map[string]*PathMapType)
	return pathMap
}

// Add adds a path.
func (pmt *PathMapType) Add(path []string) {
	if len(path) <= 1 {
		return
	}
	c := path[1]
	if _, ok := pmt.Children[c]; !ok {
		pmt.Children[c] = NewPathMap(pmt.Path + parGOPathDelimiter + c)
	}
	pmt.Children[c].Add(path[1:])
}

// SchemaHandler stores the schema data.
type SchemaHandler struct {
	SchemaElements []*parquet.SchemaElement
	MapIndex       map[string]int32
	IndexMap       map[int32]string
	PathMap        *PathMapType
	Infos          []*Tag

	InPathToExPath map[string]string
	ExPathToInPath map[string]string

	ValueColumns []string
}

// setValueColumns collects leaf nodes' full path in SchemaHandler.ValueColumns.
func (sh *SchemaHandler) setValueColumns() {
	for i := 0; i < len(sh.SchemaElements); i++ {
		schema := sh.SchemaElements[i]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			pathStr := sh.IndexMap[int32(i)]
			sh.ValueColumns = append(sh.ValueColumns, pathStr)
		}
	}
}

// GetColumnNum returns number of columns.
func (sh *SchemaHandler) GetColumnNum() int64 {
	return int64(len(sh.ValueColumns))
}

// setPathMap builds the PathMap from leaf SchemaElement.
func (sh *SchemaHandler) setPathMap() {
	sh.PathMap = NewPathMap(sh.GetRootInName())
	for i := 0; i < len(sh.SchemaElements); i++ {
		schema := sh.SchemaElements[i]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			pathStr := sh.IndexMap[int32(i)]
			sh.PathMap.Add(strToPath(pathStr))
		}
	}
}

// GetRepetitionType returns the repetition type of a column by it's schema path.
func (sh *SchemaHandler) GetRepetitionType(path []string) (parquet.FieldRepetitionType, error) {
	pathStr := pathToStr(path)
	if index, ok := sh.MapIndex[pathStr]; ok {
		return sh.SchemaElements[index].GetRepetitionType(), nil
	}
	return 0, errors.New("name not in schema")
}

// MaxDefinitionLevel returns the max definition level type of a column by it's schema path.
func (sh *SchemaHandler) MaxDefinitionLevel(path []string) (int32, error) {
	var res int32
	ln := len(path)
	for i := 2; i <= ln; i++ {
		rt, err := sh.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}
		if rt != parquet.FieldRepetitionType_REQUIRED {
			res++
		}
	}
	return res, nil
}

// GetRepetitionLevelIndex returns the max repetition level type of a column by it's schema path.
func (sh *SchemaHandler) GetRepetitionLevelIndex(path []string, rl int32) (int32, error) {
	var res int32
	ln := len(path)
	for i := 2; i <= ln; i++ {
		rt, err := sh.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}
		if rt == parquet.FieldRepetitionType_REPEATED {
			res++
		}

		if res == rl {
			return int32(i - 1), nil
		}
	}
	return res, fmt.Errorf("rl = %d not found in path = %v", rl, path)
}

// MaxRepetitionLevel returns the max repetition level type of a column by it's schema path.
func (sh *SchemaHandler) MaxRepetitionLevel(path []string) (int32, error) {
	var res int32
	ln := len(path)
	for i := 2; i <= ln; i++ {
		rt, err := sh.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}
		if rt == parquet.FieldRepetitionType_REPEATED {
			res++
		}
	}
	return res, nil
}

// GetInName returns in name from index.
func (sh *SchemaHandler) GetInName(index int) string {
	return sh.Infos[index].InName
}

// GetExName returns ex name from index.
func (sh *SchemaHandler) GetExName(index int) string {
	return sh.Infos[index].ExName
}

// CreateInExMap maps in and ex fields.
func (sh *SchemaHandler) CreateInExMap() {
	// use DFS get path of schema
	sh.ExPathToInPath, sh.InPathToExPath = map[string]string{}, map[string]string{}
	schemas := sh.SchemaElements
	ln := int32(len(schemas))
	var pos int32
	stack := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}
			item := [2]int32{pos, schemas[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else { // leaf node
			inPath, exPath := make([]string, 0), make([]string, 0)
			for i := 0; i < len(stack); i++ {
				inPath = append(inPath, sh.Infos[stack[i][0]].InName)
				exPath = append(exPath, sh.Infos[stack[i][0]].ExName)

				inPathStr, exPathStr := pathToStr(inPath), pathToStr(exPath)
				sh.ExPathToInPath[exPathStr] = inPathStr
				sh.InPathToExPath[inPathStr] = exPathStr
			}
			stack = stack[:len(stack)-1]
		}
	}
}

// ConvertToInPathStr converts a path to internal path.
func (sh *SchemaHandler) ConvertToInPathStr(pathStr string) (string, error) {
	if _, ok := sh.InPathToExPath[pathStr]; ok {
		return pathStr, nil
	}

	if res, ok := sh.ExPathToInPath[pathStr]; ok {
		return res, nil
	}

	return "", fmt.Errorf("can't find path %v", pathStr)
}

// GetRootInName get root name from the schema handler.
func (sh *SchemaHandler) GetRootInName() string {
	if len(sh.SchemaElements) == 0 {
		return ""
	}
	return sh.Infos[0].InName
}

// GetRootExName get root ex name from the schema handler.
func (sh *SchemaHandler) GetRootExName() string {
	if len(sh.SchemaElements) == 0 {
		return ""
	}
	return sh.Infos[0].ExName
}

// Item represents a field.
type Item struct {
	GoType reflect.Type
	Info   *Tag
}

// NewItem returns a new item.
func NewItem() *Item {
	return &Item{
		Info: NewTag(),
	}
}

// NewSchemaHandlerFromStruct create schema handler from a object.
func NewSchemaHandlerFromStruct(obj interface{}) (sh *SchemaHandler, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("error occurred")
			}
		}
	}()

	ot := reflect.TypeOf(obj).Elem()
	item := NewItem()
	item.GoType = ot
	item.Info.InName = "Parquet_go_root"
	item.Info.ExName = "parquet_go_root"
	item.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED

	stack := make([]*Item, 1)
	stack[0] = item
	schemaElements := make([]*parquet.SchemaElement, 0)
	infos := make([]*Tag, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item = stack[ln-1]
		stack = stack[:ln-1]
		var newInfo *Tag

		switch {
		case item.GoType.Kind() == reflect.Struct:
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			schema.RepetitionType = &item.Info.RepetitionType
			numField := int32(item.GoType.NumField())
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)

			newInfo = NewTag()
			DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)

			for i := int(numField - 1); i >= 0; i-- {
				f := item.GoType.Field(i)
				tagStr := f.Tag.Get("parquet")

				// ignore item without parquet tag
				if len(tagStr) == 0 {
					numField--
					continue
				}

				newItem := NewItem()
				newItem.Info, err = StringToTag(tagStr)
				if err != nil {
					return nil, fmt.Errorf("failed parse tag: %w", err)
				}
				newItem.Info.InName = f.Name
				newItem.GoType = f.Type
				if f.Type.Kind() == reflect.Ptr {
					newItem.GoType = f.Type.Elem()
					newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
				}
				stack = append(stack, newItem)
			}
		case item.GoType.Kind() == reflect.Slice &&
			item.Info.RepetitionType != parquet.FieldRepetitionType_REPEATED:
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			rt1 := item.Info.RepetitionType
			schema.RepetitionType = &rt1
			var numField int32 = 1
			schema.NumChildren = &numField
			ct1 := parquet.ConvertedType_LIST
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			newInfo = NewTag()
			DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "List"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)
			newInfo = NewTag()
			DeepCopy(item.Info, newInfo)
			newInfo.InName, newInfo.ExName = "List", "list"
			infos = append(infos, newInfo)

			newItem := NewItem()
			newItem.Info = GetValueTagMap(item.Info)
			newItem.Info.InName = "Element"
			newItem.Info.ExName = "element"
			newItem.GoType = item.GoType.Elem()
			if newItem.GoType.Kind() == reflect.Ptr {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
				newItem.GoType = item.GoType.Elem().Elem()
			} else {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			}
			stack = append(stack, newItem)

		case item.GoType.Kind() == reflect.Slice &&
			item.Info.RepetitionType == parquet.FieldRepetitionType_REPEATED:

			newItem := NewItem()
			newItem.Info = item.Info
			newItem.GoType = item.GoType.Elem()
			stack = append(stack, newItem)

		case item.GoType.Kind() == reflect.Map:
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			rt1 := item.Info.RepetitionType
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			ct1 := parquet.ConvertedType_MAP
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			newInfo = NewTag()
			DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "Key_value"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			var numField2 int32 = 2
			schema.NumChildren = &numField2
			ct2 := parquet.ConvertedType_MAP_KEY_VALUE
			schema.ConvertedType = &ct2
			schemaElements = append(schemaElements, schema)
			newInfo = NewTag()
			DeepCopy(item.Info, newInfo)
			newInfo.InName, newInfo.ExName = "Key_value", "key_value"
			infos = append(infos, newInfo)

			newItem := NewItem()
			newItem.Info = GetValueTagMap(item.Info)
			newItem.GoType = item.GoType.Elem()
			if newItem.GoType.Kind() == reflect.Ptr {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
				newItem.GoType = item.GoType.Elem().Elem()
			} else {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			}
			stack = append(stack, newItem)

			newItem = NewItem()
			newItem.Info = GetKeyTagMap(item.Info)
			newItem.GoType = item.GoType.Key()
			newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			stack = append(stack, newItem)

		default:
			schema, err := NewSchemaElementFromTagMap(item.Info)
			if err != nil {
				return nil, fmt.Errorf("failed to create schema from tag map: %w", err)
			}
			schemaElements = append(schemaElements, schema)
			newInfo = NewTag()
			DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)
		}
	}

	res := NewSchemaHandlerFromSchemaList(schemaElements)
	res.Infos = infos
	res.CreateInExMap()
	return res, nil
}

// NewSchemaHandlerFromSchemaList creates schema handler from schema list.
func NewSchemaHandlerFromSchemaList(schemas []*parquet.SchemaElement) *SchemaHandler {
	schemaHandler := new(SchemaHandler)
	schemaHandler.MapIndex = make(map[string]int32)
	schemaHandler.IndexMap = make(map[int32]string)
	schemaHandler.InPathToExPath = make(map[string]string)
	schemaHandler.ExPathToInPath = make(map[string]string)
	schemaHandler.SchemaElements = schemas

	schemaHandler.Infos = make([]*Tag, len(schemas))
	for i := 0; i < len(schemas); i++ {
		name := schemas[i].GetName()
		InName, ExName := StringToVariableName(name), name
		schemaHandler.Infos[i] = &Tag{
			InName: InName,
			ExName: ExName,
		}
	}
	schemaHandler.CreateInExMap()

	// use DFS get path of schema
	ln := int32(len(schemas))
	var pos int32
	stack := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}
			item := [2]int32{pos, schemas[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else {
			path := make([]string, 0)
			for i := 0; i < len(stack); i++ {
				inname := schemaHandler.Infos[stack[i][0]].InName
				path = append(path, inname)
			}
			topPos := stack[len(stack)-1][0]
			schemaHandler.MapIndex[pathToStr(path)] = topPos
			schemaHandler.IndexMap[topPos] = pathToStr(path)
			stack = stack[:len(stack)-1]
		}
	}
	schemaHandler.setPathMap()
	schemaHandler.setValueColumns()

	return schemaHandler
}
