package aggregates

import (
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func jsonEncode(v interface{}) (p []byte) {

	if v == nil {
		panic(errors.New("aggregates json encode failed, v is nil"))
	}

	var err error
	p, err = json.Marshal(v)

	if err != nil {
		panic(fmt.Errorf("aggregates json encode failed, %v", err))
	}

	return
}

func jsonDecode(p []byte, v interface{}) (err error) {

	err = json.Unmarshal(p, v)
	if err != nil {
		err = fmt.Errorf("aggregates json decode failed, %v", err)
		return
	}

	return
}
