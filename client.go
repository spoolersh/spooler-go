package spooler

import (
	"cmp"
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/spoolersh/spooler-go/internal/httputil"
)

var (
	DefaultEndpoint = "api.spooler.sh"
)

type Client struct {
	Trace    ClientTrace
	Endpoint string
	APIKey   string

	once sync.Once
	http *httputil.Client
}

func (c *Client) init() {
	c.once.Do(func() {
		u := url.URL{
			Scheme: "https",
			Host:   cmp.Or(c.Endpoint, DefaultEndpoint),
			Path:   "v1",
		}
		c.http = &httputil.Client{
			UserAgent: "spooler-go/" + Version,
			BaseURL:   u.String(),
			Header: http.Header{
				"Authorization": []string{"Bearer " + c.APIKey},
			},
		}
	})
}

func (c *Client) SendFrom(ctx context.Context, r io.Reader) {
	c.init()
}

func (c *Client) RecvTo(ctx context.Context, w io.Writer) {
	c.init()
}
