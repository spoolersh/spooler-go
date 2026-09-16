package httputil

//go:generate gtrace

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
)

//gtrace:gen
type ClientTrace struct {
	RoundTripper RoundTripperTrace
}

//gtrace:gen
type RoundTripperTrace struct {
	OnRoundTrip func(*http.Request) func(*http.Response, error)
	OnConnect   func(network, addr string) func(error)
}

func LoggerRoundTripperTrace(logger *slog.Logger) RoundTripperTrace {
	return RoundTripperTrace{
		OnRoundTrip: func(req *http.Request) func(*http.Response, error) {
			logger.Debug("doing http request",
				"method", req.Method,
				"url", req.URL,
			)
			return func(res *http.Response, err error) {
				if err != nil {
					logger.Error("http request failed",
						"error", err,
					)
					return
				}
				logger.Debug(
					"http request done",
					"status", res.Status,
				)
			}
		},
		OnConnect: func(network, addr string) func(error) {
			return func(err error) {
			}
		},
	}
}

// DumpRoundTripperTrace writes every request and response to w in wire form.
// The named headers are shown as REDACTED, in both directions, so that a
// credential never lands in a log. The dump helpers consume and replace a
// body, so they run on the real message with its headers swapped for the
// duration of the dump.
func DumpRoundTripperTrace(w io.Writer, redact ...string) RoundTripperTrace {
	return RoundTripperTrace{
		OnRoundTrip: func(req *http.Request) func(*http.Response, error) {
			orig := req.Header
			req.Header = redactHeader(orig, redact)
			bts, _ := httputil.DumpRequestOut(req, true)
			req.Header = orig
			fmt.Fprintln(w, string(bts))
			return func(res *http.Response, err error) {
				if err != nil {
					return
				}
				orig := res.Header
				res.Header = redactHeader(orig, redact)
				bts, _ := httputil.DumpResponse(res, true)
				res.Header = orig
				fmt.Fprintln(w, string(bts))
			}
		},
	}
}

// redactHeader returns a copy of h with the named headers' values replaced.
func redactHeader(h http.Header, names []string) http.Header {
	out := h.Clone()
	for _, name := range names {
		if _, has := out[http.CanonicalHeaderKey(name)]; has {
			out.Set(name, "REDACTED")
		}
	}
	return out
}
