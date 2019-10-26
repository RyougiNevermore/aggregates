package aggregates

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func NewMetaData() *MetaData {
	return &MetaData{value:make(map[string]interface{})}
}

type MetaData struct {
	value map[string]interface{}
}

func (m *MetaData) Get(key string) (data interface{}, has bool) {
	data, has = m.value[key]
	return
}

func (m *MetaData) GetString(key string) (data string, has bool) {
	var v interface{}
	v, has = m.value[key]
	if !has {
		return
	}
	isStr := false
	if data, isStr = v.(string); isStr {
		return
 	} else {
 		data = fmt.Sprint(v)
	}
	return
}

func (m *MetaData) GetInt(key string) (data int, has bool, err error) {
	var v interface{}
	v, has = m.value[key]
	if !has {
		return
	}
	isStr := false
	if data, isStr = v.(int); isStr {
		return
	} else {
		err = errors.New("data is not int type")
	}
	return
}

func (m *MetaData) GetFloat64(key string) (data float64, has bool, err error) {
	var v interface{}
	v, has = m.value[key]
	if !has {
		return
	}
	isStr := false
	if data, isStr = v.(float64); isStr {
		return
	} else {
		err = errors.New("data is not float64 type")
	}
	return
}

func (m *MetaData) GetTime(key string) (data time.Time, has bool, err error) {
	var v interface{}
	v, has = m.value[key]
	if !has {
		return
	}
	isStr := false
	if data, isStr = v.(time.Time); isStr {
		return
	} else {
		err = errors.New("data is not time.Time type")
	}
	return
}

func (m *MetaData) ToInterfaceViaJson() (v interface{}, err error) {
	p, merr := json.Marshal(m.value)
	if merr != nil {
		err = errors.New("can not marshal metadata to json")
		return
	}
	umerr := json.Unmarshal(p, v)
	if umerr != nil {
		err = errors.New("can not unmarshal metadata to interface")
	}
	return
}

func (m *MetaData) DataSet() (data []interface{}) {
	data = make([]interface{}, len(m.value))
	for _, v := range m.value {
		data = append(data, v)
	}
	return
}

func (m *MetaData) Size() (size int) {
	size = len(m.value)
	return
}

func (m *MetaData) Range(fn func(key string, data interface{})) {
	for k, v := range m.value {
		fn(k, v)
	}
	return
}




