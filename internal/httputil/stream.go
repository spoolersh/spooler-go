package httputil

import (
	"fmt"
	"io"
)

var _streamCodec = new(streamCodec)

var (
	StreamEncoder Encoder = _streamCodec
	StreamDecoder Decoder = _streamCodec
)

type streamCodec struct {
}

func (j *streamCodec) MediaType() string { return "application/octet-stream" }

type sizer interface {
	Size() int64
}

// Encode sizes the source. A Size method caps what is sent; a seeker is also
// measured from its current offset to the end, since the standard readers'
// Size is the whole underlying data, not what is left to read. A bare reader
// without Size has an unknown size.
func (j *streamCodec) Encode(v any) (Body, error) {
	var size int64 = -1
	if s, ok := v.(sizer); ok {
		size = s.Size()
	}
	switch x := v.(type) {
	case io.ReadSeeker:
		offset, err := x.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		end, err := x.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
		_, err = x.Seek(offset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		remaining := end - offset
		if size < 0 || size > remaining {
			size = remaining
		}
		return &streamBody{
			reader: x,
			seeker: x,
			offset: offset,
			size:   size,
		}, nil

	case io.Reader:
		return &streamBody{
			reader: x,
			size:   size,
		}, nil

	default:
		return nil, fmt.Errorf(
			"%T does not implement io.Reader",
			v,
		)
	}
}

func (h *streamBody) Open() (rc io.ReadCloser, err error) {
	if h.used && h.seeker == nil {
		return nil, fmt.Errorf("stream: can't reopen the body")
	}
	if h.used && h.seeker != nil {
		_, err := h.seeker.Seek(h.offset, io.SeekStart)
		if err != nil {
			return nil, err
		}
	}
	//if h.used && h.reseter != nil {
	//	err := h.reseter.Reset()
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	c, ok := h.reader.(io.Closer)
	if !ok {
		c = io.NopCloser(nil)
	}
	h.used = true
	// A known size bounds the read; an unknown one reads to EOF.
	r := h.reader
	if h.size >= 0 {
		r = io.LimitReader(r, h.size)
	}
	return struct {
		io.Reader
		io.Closer
	}{
		r,
		c,
	}, nil
}

func (j *streamCodec) Decode(v any, r io.Reader) (err error) {
	switch x := v.(type) {
	case io.ReaderFrom:
		_, err = x.ReadFrom(r)
	case io.Writer:
		_, err = io.Copy(x, r)
	default:
		return fmt.Errorf(
			"%T does not implement io.ReaderFrom or io.Writer",
			v,
		)
	}
	return err
}

type streamBody struct {
	reader io.Reader
	seeker io.Seeker
	offset int64
	size   int64 // -1 if unknown.
	used   bool
}

func (h *streamBody) Size() int64 {
	return h.size
}
