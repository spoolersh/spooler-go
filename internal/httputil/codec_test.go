package httputil

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// closeRecorder is a reader that records whether it was closed.
type closeRecorder struct {
	io.Reader
	closed bool
}

func (c *closeRecorder) Close() error {
	c.closed = true
	return nil
}

// unsizedReader hides every optional method of a reader, so that the codec
// sees a bare io.Reader of unknown size.
type unsizedReader struct {
	io.Reader
}

// sizedReader is a bare reader that declares its size.
type sizedReader struct {
	io.Reader
	size int64
}

func (s sizedReader) Size() int64 { return s.size }

// limitedSeeker is a seeker that declares its size.
type limitedSeeker struct {
	io.ReadSeeker
	size int64
}

func (s limitedSeeker) Size() int64 { return s.size }

// TestStreamEncode covers how the stream codec sizes a source and whether it
// can open it again for a retry.
func TestStreamEncode(t *testing.T) {
	for _, test := range []struct {
		name     string
		src      any
		expSize  int64
		expBody  string // What the first Open reads.
		reopens  bool   // Whether a second Open yields the same body.
		err      bool
		closable bool // Whether Close reaches the source.
	}{
		{
			name:    "a read seeker is sized by seeking and rewinds for a retry",
			src:     strings.NewReader("hello"),
			expSize: 5,
			expBody: "hello",
			reopens: true,
		},
		{
			name: "a read seeker is sized from its current offset",
			src: func() io.ReadSeeker {
				r := strings.NewReader("hello")
				r.Seek(2, io.SeekStart)
				return r
			}(),
			expSize: 3,
			expBody: "llo",
			reopens: true,
		},
		{
			name: "a declared size bounds the read of a bare reader",
			src: sizedReader{
				Reader: strings.NewReader("hello world"),
				size:   5,
			},
			expSize: 5,
			expBody: "hello",
		},
		{
			name: "a declared size bounds the read of a seeker",
			src: limitedSeeker{
				ReadSeeker: strings.NewReader("hello world"),
				size:       5,
			},
			expSize: 5,
			expBody: "hello",
			reopens: true,
		},
		{
			name: "a declared size beyond what is left is cut to what is left",
			src: limitedSeeker{
				ReadSeeker: strings.NewReader("hello"),
				size:       99,
			},
			expSize: 5,
			expBody: "hello",
			reopens: true,
		},
		{
			name: "a reader of unknown size reads to the end and cannot be reopened",
			src: unsizedReader{
				Reader: strings.NewReader("hello"),
			},
			expSize: -1,
			expBody: "hello",
		},
		{
			name: "a closer is closed with the body",
			src: &closeRecorder{
				Reader: strings.NewReader("hello"),
			},
			expSize:  -1,
			expBody:  "hello",
			closable: true,
		},
		{
			name: "a seeker that cannot tell its offset",
			src: &failingSeeker{
				ReadSeeker: strings.NewReader("hello"),
				failAt:     1,
			},
			err: true,
		},
		{
			name: "a seeker that cannot find its end",
			src: &failingSeeker{
				ReadSeeker: strings.NewReader("hello"),
				failAt:     2,
			},
			err: true,
		},
		{
			name: "a seeker that cannot return to its offset",
			src: &failingSeeker{
				ReadSeeker: strings.NewReader("hello"),
				failAt:     3,
			},
			err: true,
		},
		{
			name: "a seeker that cannot rewind for a retry",
			src: &failingSeeker{
				ReadSeeker: strings.NewReader("hello"),
				failAt:     4,
			},
			expSize: 5,
			expBody: "hello",
		},
		{
			name: "not a reader",
			src:  42,
			err:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, err := StreamEncoder.Encode(test.src)
			if test.err {
				if err == nil {
					t.Errorf("want error; got nothing")
				}
				return
			}
			if err != nil {
				t.Fatalf(
					"unexpected error: %v",
					err,
				)
			}
			if act, exp := body.Size(), test.expSize; act != exp {
				t.Errorf(
					"size: %d; want %d",
					act, exp,
				)
			}
			act := readBody(t, body)
			if act, exp := act, test.expBody; act != exp {
				t.Errorf(
					"body: %q; want %q",
					act, exp,
				)
			}
			if c, ok := test.src.(*closeRecorder); ok {
				if act, exp := c.closed, test.closable; act != exp {
					t.Errorf(
						"closed: %t; want %t",
						act, exp,
					)
				}
			}
			_, err = body.Open()
			if test.reopens {
				if err != nil {
					t.Fatalf(
						"unexpected reopen error: %v",
						err,
					)
				}
				if act, exp := readBody(t, body), test.expBody; act != exp {
					t.Errorf(
						"reopened body: %q; want %q",
						act, exp,
					)
				}
				return
			}
			if err == nil {
				t.Errorf("want reopen error; got nothing")
			}
		})
	}
}

// failingSeeker is a seeker whose n-th seek fails. Encode seeks three times
// (current, end, back to current) and each reopen seeks once more.
type failingSeeker struct {
	io.ReadSeeker
	failAt int
	seeks  int
}

func (s *failingSeeker) Seek(offset int64, whence int) (int64, error) {
	s.seeks++
	if s.seeks == s.failAt {
		return 0, errBoom
	}
	return s.ReadSeeker.Seek(offset, whence)
}

func readBody(t *testing.T, body Body) string {
	t.Helper()
	rc, err := body.Open()
	if err != nil {
		t.Fatalf(
			"unexpected open error: %v",
			err,
		)
	}
	defer rc.Close()
	p, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf(
			"unexpected read error: %v",
			err,
		)
	}
	return string(p)
}

func TestStreamDecode(t *testing.T) {
	for _, test := range []struct {
		name string
		into any
		exp  string
		err  bool
	}{
		{
			name: "a writer",
			into: new(strings.Builder),
			exp:  "payload",
		},
		{
			name: "a reader-from",
			into: new(bytes.Buffer),
			exp:  "payload",
		},
		{
			name: "neither",
			into: new(int),
			err:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := StreamDecoder.Decode(test.into, strings.NewReader("payload"))
			if test.err {
				if err == nil {
					t.Errorf("want error; got nothing")
				}
				return
			}
			if err != nil {
				t.Fatalf(
					"unexpected error: %v",
					err,
				)
			}
			var act string
			switch v := test.into.(type) {
			case *strings.Builder:
				act = v.String()
			case *bytes.Buffer:
				act = v.String()
			}
			if act, exp := act, test.exp; act != exp {
				t.Errorf(
					"decoded: %q; want %q",
					act, exp,
				)
			}
		})
	}
}

func TestJSONCodec(t *testing.T) {
	t.Run("encode", func(t *testing.T) {
		for _, test := range []struct {
			name string
			v    any
			exp  string
			err  bool
		}{
			{
				name: "a value",
				v: map[string]int{
					"a": 1,
				},
				exp: `{"a":1}`,
			},
			{
				name: "a value that cannot be marshalled",
				v:    make(chan int),
				err:  true,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				body, err := JSONEncoder.Encode(test.v)
				if test.err {
					if err == nil {
						t.Errorf("want error; got nothing")
					}
					return
				}
				if err != nil {
					t.Fatalf(
						"unexpected error: %v",
						err,
					)
				}
				if act, exp := body.Size(), int64(len(test.exp)); act != exp {
					t.Errorf(
						"size: %d; want %d",
						act, exp,
					)
				}
				if act, exp := readBody(t, body), test.exp; act != exp {
					t.Errorf(
						"body: %q; want %q",
						act, exp,
					)
				}
				// A JSON body is bytes and is always replayable.
				if act, exp := readBody(t, body), test.exp; act != exp {
					t.Errorf(
						"reopened body: %q; want %q",
						act, exp,
					)
				}
			})
		}
	})
	t.Run("decode", func(t *testing.T) {
		for _, test := range []struct {
			name string
			json string
			exp  map[string]any
			err  bool
		}{
			{
				name: "a value",
				json: `{"a":1}`,
				exp: map[string]any{
					"a": 1.0,
				},
			},
			{
				name: "garbage",
				json: "garbage",
				err:  true,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				var act map[string]any
				err := JSONDecoder.Decode(&act, strings.NewReader(test.json))
				if test.err {
					if err == nil {
						t.Errorf("want error; got nothing")
					}
					return
				}
				if err != nil {
					t.Fatalf(
						"unexpected error: %v",
						err,
					)
				}
				if diff := cmp.Diff(test.exp, act); diff != "" {
					t.Errorf(
						"decoded mismatch (-want +act):\n%s",
						diff,
					)
				}
			})
		}
	})
	if act, exp := JSONEncoder.MediaType(), "application/json"; act != exp {
		t.Errorf(
			"media type: %q; want %q",
			act, exp,
		)
	}
	if act, exp := StreamEncoder.MediaType(), "application/octet-stream"; act != exp {
		t.Errorf(
			"media type: %q; want %q",
			act, exp,
		)
	}
}

func TestCodecsMerge(t *testing.T) {
	base := Codecs{
		Send:  JSONEncoder,
		Recv:  JSONDecoder,
		Error: JSONDecoder,
	}
	for _, test := range []struct {
		name string
		over Codecs
		exp  Codecs
	}{
		{
			name: "nothing set keeps the base",
			exp:  base,
		},
		{
			name: "what is set wins, the rest falls back",
			over: Codecs{
				Send: StreamEncoder,
			},
			exp: Codecs{
				Send:  StreamEncoder,
				Recv:  JSONDecoder,
				Error: JSONDecoder,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			act := test.over.Merge(base)
			if act != test.exp {
				t.Errorf(
					"codecs: %+v; want %+v",
					act, test.exp,
				)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, test := range []struct {
		name string
		str  string
		exp  time.Duration
		err  bool
	}{
		{
			name: "seconds",
			str:  "30",
			exp:  30 * time.Second,
		},
		{
			name: "zero",
			str:  "0",
		},
		{
			name: "a signed number is malformed",
			str:  "-5",
			err:  true,
		},
		{
			name: "a plus sign is malformed too",
			str:  "+5",
			err:  true,
		},
		{
			name: "a date in the future",
			str:  "Fri, 02 Jan 2026 03:04:15 GMT",
			exp:  10 * time.Second,
		},
		{
			name: "a date in the past is now",
			str:  "Fri, 02 Jan 2026 03:03:55 GMT",
		},
		{
			name: "garbage",
			str:  "soon",
			err:  true,
		},
		{
			name: "empty",
			err:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			act, err := ParseRetryAfter(now, test.str)
			if test.err {
				if err == nil {
					t.Errorf("want error; got nothing")
				}
				return
			}
			if err != nil {
				t.Fatalf(
					"unexpected error: %v",
					err,
				)
			}
			if act, exp := act, test.exp; act != exp {
				t.Errorf(
					"delay: %v; want %v",
					act, exp,
				)
			}
		})
	}
}

func TestStatusError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  *StatusError
		text string
		wrap error
	}{
		{
			name: "a bare status",
			err: &StatusError{
				Code: 503,
			},
			text: "httputil: unsuccessful status code: 503",
		},
		{
			name: "a status with a decoded error",
			err: &StatusError{
				Code: 500,
				Desc: errBoom,
			},
			text: "httputil: unsuccessful status code: 500: boom",
			wrap: errBoom,
		},
		{
			name: "a status with a description that is not an error",
			err: &StatusError{
				Code: 404,
				Desc: &clientErrBody{
					Kind: "gone",
				},
			},
			text: "httputil: unsuccessful status code: 404",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if act, exp := test.err.Error(), test.text; act != exp {
				t.Errorf(
					"text: %q; want %q",
					act, exp,
				)
			}
			if act, exp := errors.Unwrap(test.err), test.wrap; act != exp {
				t.Errorf(
					"unwrap: %v; want %v",
					act, exp,
				)
			}
		})
	}
}
