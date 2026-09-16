package httputil

//go:generate gtrace

import (
	"log/slog"
	"net/http"
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

func DumpRoundTripperTrace() RoundTripperTrace {
	return RoundTripperTrace{
		OnRoundTrip: func(req *http.Request) func(*http.Response, error) {
			return nil
			//bts, _ := httputil.DumpRequestOut(req, false)
			//fmt.Fprintln(os.Stderr, string(bts))
			//return func(res *http.Response, err error) {
			//	if err != nil {
			//		return
			//	}
			//	bts, _ := httputil.DumpResponse(res, false)
			//	fmt.Fprintln(os.Stderr, string(bts))
			//}
		},
	}
}
