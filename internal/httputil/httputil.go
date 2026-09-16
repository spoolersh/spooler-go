package httputil

import (
	"net/http"
	"net/http/httptrace"
)

type TracingRoundTripper struct {
	Trace RoundTripperTrace
	Next  http.RoundTripper
}

func (r *TracingRoundTripper) RoundTrip(req *http.Request) (res *http.Response, err error) {
	done := r.Trace.onRoundTrip(req)
	defer func() {
		done(res, err)
	}()

	var connectDone func(error)
	ctx := httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		ConnectStart: func(network, addr string) {
			connectDone = r.Trace.onConnect(network, addr)
		},
		ConnectDone: func(_, _ string, err error) {
			connectDone(err)
		},
	})
	defer func() {

	}()
	return r.Next.RoundTrip(req.WithContext(ctx))
}

type RoundTripperFunc func(*http.Request) (*http.Response, error)

func (f RoundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
