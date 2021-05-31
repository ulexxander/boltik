package boltik

import (
	"bytes"
	"encoding/json"
)

var (
	jsonArraySeparator = []byte(",")
	jsonArrayStart     = []byte("[")
	jsonArrayEnd       = []byte("]")
)

type Codec interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
	Join(data [][]byte) []byte
}

type CodecJSON struct{}

func NewCodecJSON() *CodecJSON {
	return &CodecJSON{}
}

// impl: cj *CodecJSON boltik.Codec

func (cj *CodecJSON) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (cj *CodecJSON) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (cj *CodecJSON) Join(data [][]byte) []byte {
	itemsCount := len(data)
	if itemsCount == 0 {
		return nil
	}

	data[0] = append(jsonArrayStart, data[0]...)
	data[itemsCount-1] = append(data[itemsCount-1], jsonArrayEnd...)

	return bytes.Join(data, jsonArraySeparator)
}
