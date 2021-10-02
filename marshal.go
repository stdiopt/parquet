package parquet

import (
	"errors"
	"reflect"
	"strings"

	"github.com/stdiopt/parquet/internal/parquet"
)

// marshalNode (internal?)
type marshalNode struct {
	Val     reflect.Value
	PathMap *PathMapType
	RL      int32
	DL      int32
}

// Improve Performance///////////////////////////
// marshalNodeBufType ...
type marshalNodeBufType struct {
	Index int
	Buf   []*marshalNode
}

// newNodeBuf returns a NodeBufType.
func newNodeBuf(ln int) *marshalNodeBufType {
	nodeBuf := new(marshalNodeBufType)
	nodeBuf.Index = 0
	nodeBuf.Buf = make([]*marshalNode, ln)
	for i := 0; i < ln; i++ {
		nodeBuf.Buf[i] = new(marshalNode)
	}
	return nodeBuf
}

// GetNode returns a node.
func (nbt *marshalNodeBufType) GetNode() *marshalNode {
	if nbt.Index >= len(nbt.Buf) {
		nbt.Buf = append(nbt.Buf, new(marshalNode))
	}
	nbt.Index++
	return nbt.Buf[nbt.Index-1]
}

// Reset  resets node index.
func (nbt *marshalNodeBufType) Reset() {
	nbt.Index = 0
}

// Marshaler interface with MarshalMethod.
type Marshaler interface {
	Marshal(node *marshalNode, nodeBuf *marshalNodeBufType) []*marshalNode
}

type parquetPtr struct{}

func (p *parquetPtr) Marshal(node *marshalNode, nodeBuf *marshalNodeBufType) []*marshalNode {
	nodes := make([]*marshalNode, 0)
	if node.Val.IsNil() {
		return nodes
	}
	node.Val = node.Val.Elem()
	node.DL++
	nodes = append(nodes, node)
	return nodes
}

type parquetStruct struct{}

func (p *parquetStruct) Marshal(node *marshalNode, nodeBuf *marshalNodeBufType) []*marshalNode {
	var ok bool

	numField := node.Val.Type().NumField()
	nodes := make([]*marshalNode, 0, numField)
	for j := 0; j < numField; j++ {
		tf := node.Val.Type().Field(j)
		name := tf.Name
		newNode := nodeBuf.GetNode()

		// some ignored item
		if newNode.PathMap, ok = node.PathMap.Children[name]; !ok {
			continue
		}

		newNode.Val = node.Val.Field(j)
		newNode.RL = node.RL
		newNode.DL = node.DL
		nodes = append(nodes, newNode)
	}
	return nodes
}

type parquetMapStruct struct{}

func (p *parquetMapStruct) Marshal(node *marshalNode, nodeBuf *marshalNodeBufType) []*marshalNode {
	var ok bool

	nodes := make([]*marshalNode, 0)
	keys := node.Val.MapKeys()
	if len(keys) == 0 {
		return nodes
	}

	for j := len(keys) - 1; j >= 0; j-- {
		key := keys[j]
		newNode := nodeBuf.GetNode()

		// some ignored item
		if newNode.PathMap, ok = node.PathMap.Children[key.String()]; !ok {
			continue
		}

		newNode.Val = node.Val.MapIndex(key)
		newNode.RL = node.RL
		newNode.DL = node.DL
		nodes = append(nodes, newNode)
	}
	return nodes
}

type parquetSlice struct {
	schemaHandler *SchemaHandler
}

func (p *parquetSlice) Marshal(node *marshalNode, nodeBuf *marshalNodeBufType) []*marshalNode {
	nodes := make([]*marshalNode, 0)
	pathMap := node.PathMap
	path := node.PathMap.Path
	idx := p.schemaHandler.MapIndex[node.PathMap.Path]
	if *p.schemaHandler.SchemaElements[idx].RepetitionType != parquet.FieldRepetitionType_REPEATED {
		pathMap = pathMap.Children["List"].Children["Element"]
		path = path + parGOPathDelimiter + "List" + parGOPathDelimiter + "Element"
	}
	ln := node.Val.Len()
	if ln == 0 {
		return nodes
	}

	rlNow, _ := p.schemaHandler.MaxRepetitionLevel(strToPath(path))
	for j := ln - 1; j >= 0; j-- {
		newNode := nodeBuf.GetNode()
		newNode.PathMap = pathMap
		newNode.Val = node.Val.Index(j)
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		newNode.DL = node.DL + 1
		nodes = append(nodes, newNode)
	}
	return nodes
}

type parquetMap struct {
	schemaHandler *SchemaHandler
}

func (p *parquetMap) Marshal(node *marshalNode, nodeBuf *marshalNodeBufType) []*marshalNode {
	nodes := make([]*marshalNode, 0)
	path := node.PathMap.Path + parGOPathDelimiter + "Key_value"
	keys := node.Val.MapKeys()
	if len(keys) == 0 {
		return nodes
	}

	rlNow, _ := p.schemaHandler.MaxRepetitionLevel(strToPath(path))
	for j := len(keys) - 1; j >= 0; j-- {
		key := keys[j]
		value := node.Val.MapIndex(key)
		newNode := nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["Key_value"].Children["Key"]
		newNode.Val = key
		newNode.DL = node.DL + 1
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		nodes = append(nodes, newNode)

		newNode = nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["Key_value"].Children["Value"]
		newNode.Val = value
		newNode.DL = node.DL + 1
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		nodes = append(nodes, newNode)
	}
	return nodes
}

// marshal convert the objects to table map. srcInterface is a slice of objects.
func marshal(srcInterface []interface{}, schemaHandler *SchemaHandler) (tb *map[string]*layoutTable, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown error")
			}
		}
	}()

	src := reflect.ValueOf(srcInterface)
	res := make(map[string]*layoutTable)
	pathMap := schemaHandler.PathMap
	nodeBuf := newNodeBuf(1)

	for i := 0; i < len(schemaHandler.SchemaElements); i++ {
		schema := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res[pathStr] = newLayoutEmptyTable()
			res[pathStr].Path = strToPath(pathStr)
			res[pathStr].MaxDefinitionLevel, _ = schemaHandler.MaxDefinitionLevel(res[pathStr].Path)
			res[pathStr].MaxRepetitionLevel, _ = schemaHandler.MaxRepetitionLevel(res[pathStr].Path)
			res[pathStr].RepetitionType = schema.GetRepetitionType()
			res[pathStr].Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
			res[pathStr].Info = schemaHandler.Infos[i]
		}
	}

	stack := make([]*marshalNode, 0, 100)
	for i := 0; i < len(srcInterface); i++ {
		stack = stack[:0]
		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		node.Val = src.Index(i)
		if src.Index(i).Type().Kind() == reflect.Interface {
			node.Val = src.Index(i).Elem()
		}
		node.PathMap = pathMap
		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node := stack[ln-1]
			stack = stack[:ln-1]

			tk := node.Val.Type().Kind()
			var m Marshaler

			switch tk {
			case reflect.Ptr:
				m = &parquetPtr{}
			case reflect.Struct:
				m = &parquetStruct{}
			case reflect.Slice:
				m = &parquetSlice{schemaHandler: schemaHandler}
			case reflect.Map:
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				sele := schemaHandler.SchemaElements[schemaIndex]
				m = &parquetMapStruct{}
				if sele.IsSetConvertedType() {
					m = &parquetMap{schemaHandler: schemaHandler}
				}
			default:
				table := res[node.PathMap.Path]
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				schema := schemaHandler.SchemaElements[schemaIndex]
				table.Values = append(table.Values, interfaceToParquetType(node.Val.Interface(), schema.Type))
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				continue
			}

			nodes := m.Marshal(node, nodeBuf)
			if len(nodes) != 0 {
				stack = append(stack, nodes...)
				continue
			}
			path := node.PathMap.Path
			index := schemaHandler.MapIndex[path]
			numChildren := schemaHandler.SchemaElements[index].GetNumChildren()
			if numChildren > int32(0) {
				for key, table := range res {
					if strings.HasPrefix(key, path) &&
						(len(key) == len(path) || key[len(path)] == parGOPathDelimiter[0]) {
						table.Values = append(table.Values, nil)
						table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
						table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
					}
				}
				continue
			}
			table := res[path]
			table.Values = append(table.Values, nil)
			table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
			table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
		}
	}

	return &res, nil
}
