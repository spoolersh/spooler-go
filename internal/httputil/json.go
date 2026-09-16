package httputil

import (
	"bytes"
	"encoding/json"
	"io"
)

var _jsonCodec = new(jsonCodec)

var (
	JSONEncoder Encoder = _jsonCodec
	JSONDecoder Decoder = _jsonCodec
)

type jsonCodec struct {
}

func (j *jsonCodec) MediaType() string { return "application/json" }

func (j *jsonCodec) Encode(v any) (Body, error) {
	bts, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &jsonBody{bts}, nil
}
func (j *jsonCodec) Decode(v any, r io.Reader) error {
	dec := json.NewDecoder(r)
	return dec.Decode(v)
}

type jsonBody struct {
	p []byte
}

func (h *jsonBody) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(h.p)), nil
}
func (h *jsonBody) Size() int64 {
	return int64(len(h.p))
}
