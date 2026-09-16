package httputil

import (
	"cmp"
	"io"
)

type Body interface {
	// Open returns encoded value stream.
	Open() (io.ReadCloser, error)

	// Size reports the number of bytes can be read from Body()'s io.Reader.
	// If size is unknown it returns -1.
	Size() int64
}

type Encoder interface {
	MediaType() string
	Encode(any) (Body, error)
}

type Decoder interface {
	MediaType() string
	Decode(any, io.Reader) error
}

type Codecs struct {
	Send  Encoder
	Recv  Decoder
	Error Decoder
}

func (c Codecs) Merge(x Codecs) Codecs {
	return Codecs{
		Send:  cmp.Or(c.Send, x.Send),
		Recv:  cmp.Or(c.Recv, x.Recv),
		Error: cmp.Or(c.Error, x.Error),
	}
}
