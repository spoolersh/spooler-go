package httputil

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
)

// TestDumpRoundTripperTrace checks that the dump shows both directions with
// the named headers redacted, and that the message itself is left as it
// was, since the dump runs on the real request and response.
func TestDumpRoundTripperTrace(t *testing.T) {
	for _, test := range []struct {
		name    string
		redact  []string
		reqHdr  http.Header
		resHdr  http.Header
		shown   []string // Substrings the dump must contain.
		hidden  []string // Substrings it must not.
		reqBody string
		resBody string
	}{
		{
			name:   "credentials are redacted in both directions",
			redact: []string{"Authorization", "Spooler-Lease"},
			reqHdr: http.Header{
				"Authorization": {"Bearer secret-key"},
				"X-Plain":       {"visible"},
			},
			resHdr: http.Header{
				"Spooler-Lease": {"secret-lease"},
				"X-Answer":      {"yes"},
			},
			reqBody: "hello",
			resBody: "world",
			shown: []string{
				"Authorization: REDACTED",
				"X-Plain: visible",
				"Spooler-Lease: REDACTED",
				"X-Answer: yes",
				"hello",
				"world",
			},
			hidden: []string{
				"secret-key",
				"secret-lease",
			},
		},
		{
			name:   "a redacted header that is absent is not invented",
			redact: []string{"Authorization"},
			reqHdr: http.Header{
				"X-Plain": {"visible"},
			},
			shown: []string{
				"X-Plain: visible",
			},
			hidden: []string{
				"Authorization",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var dump bytes.Buffer
				// On the wire, the codec adds its own headers to the case's.
				wireHdr := test.reqHdr.Clone()
				wireHdr.Set("Accept", "text/plain")
				if test.reqBody != "" {
					wireHdr.Set("Content-Type", "text/plain")
				}
				rt, done := stubWire(t, []exchange{
					{
						req: request{
							method: "POST",
							url:    "https://api.test/v1/x",
							header: wireHdr,
							body:   test.reqBody,
						},
						res: response{
							status: 200,
							header: test.resHdr,
							body:   test.resBody,
						},
					},
				})
				c := &Client{
					ClientBuilder: ClientBuilder{
						Transport: rt,
						Trace:     DumpRoundTripperTrace(&dump, test.redact...),
					},
					BaseURL: "https://api.test/v1",
					Header:  test.reqHdr,
					Codec: Codecs{
						Send: textEncoder,
						Recv: textDecoder,
					},
				}
				var got string
				req := Request{
					Path: "/x",
					Recv: &got,
				}
				if test.reqBody != "" {
					req.Send = test.reqBody
				}
				_, err := c.Do(t.Context(), "POST", req)
				done()
				if err != nil {
					t.Fatalf(
						"unexpected error: %v",
						err,
					)
				}
				// The response reached the caller intact after the dump.
				if act, exp := got, test.resBody; act != exp {
					t.Errorf(
						"decoded: %q; want %q",
						act, exp,
					)
				}
				out := dump.String()
				for _, s := range test.shown {
					if !strings.Contains(out, s) {
						t.Errorf(
							"dump lacks %q:\n%s",
							s, out,
						)
					}
				}
				for _, s := range test.hidden {
					if strings.Contains(out, s) {
						t.Errorf(
							"dump shows %q:\n%s",
							s, out,
						)
					}
				}
			})
		})
	}
}
