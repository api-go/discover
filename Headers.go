package discover

import (
	gohttp "apigo.cc/go/http"
	"apigo.cc/go/id"
	"context"
	"net"
	"net/http"
	"strings"
)

// relayRequestHeaders creates an independent multi-value snapshot without reading a body.
func relayRequestHeaders(request *http.Request) http.Header {
	headers := make(http.Header)
	if request != nil {
		allowed := make(map[string]bool, len(RelayHeaders))
		for _, name := range RelayHeaders {
			allowed[strings.ToLower(name)] = true
		}
		for name, values := range request.Header {
			if allowed[strings.ToLower(name)] || strings.HasPrefix(strings.ToLower(name), "x-app-") {
				headers[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
			}
		}
		// A prepared ingress snapshot is authoritative. Unprepared requests retain
		// legacy raw context only; this is never a security credential or RPC target.
		if address, ok := gohttp.PreparedExternalAddress(request); ok {
			_ = address.RelayHeaders(headers)
		}
		if headers.Get(HeaderClientIp) == "" {
			ip, _, err := net.SplitHostPort(request.RemoteAddr)
			if err != nil {
				ip = request.RemoteAddr
			}
			if net.ParseIP(ip) != nil {
				headers.Set(HeaderClientIp, ip)
			}
		}
		if headers.Get(HeaderUserAgent) == "" {
			headers.Set(HeaderUserAgent, request.UserAgent())
		}
		if _, prepared := gohttp.PreparedExternalAddress(request); !prepared && headers.Get(HeaderHost) == "" {
			headers.Set(HeaderHost, request.Host)
		}
		if _, prepared := gohttp.PreparedExternalAddress(request); !prepared && headers.Get(HeaderScheme) == "" {
			scheme := "http"
			if request.TLS != nil {
				scheme = "https"
			}
			headers.Set(HeaderScheme, scheme)
		}
	}
	if headers.Get(HeaderRequestId) == "" {
		headers.Set(HeaderRequestId, id.MakeID(32))
	}
	return headers
}

// relayTransport bridges the legacy flat-header client to multi-value request metadata.
// Metadata is carried in Context, never in a mutable global client field.
type relayContextKey struct{}
type relayTransport struct{ base http.RoundTripper }

func (transport relayTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if headers, ok := request.Context().Value(relayContextKey{}).(http.Header); ok {
		request = request.Clone(request.Context())
		for name, values := range headers {
			request.Header[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
		}
	}
	return transport.base.RoundTrip(request)
}
func withRelayHeaders(ctx context.Context, headers http.Header) context.Context {
	return context.WithValue(ctx, relayContextKey{}, headers)
}

func (c *Caller) relayHeaders() http.Header {
	headers := relayRequestHeaders(c.Request)
	if c.requestID != "" && c.Request != nil && c.Request.Header.Get(HeaderRequestId) == "" {
		headers.Set(HeaderRequestId, c.requestID)
	}
	return headers
}

// PrepareExternalRequest explicitly resolves native ingress addressing and then
// prepares discovery metadata. It never changes the RPC destination or authentication.
func PrepareExternalRequest(request *http.Request, resolver *gohttp.ExternalAddressResolver) error {
	address := resolver.Prepare(request)
	if err := address.RelayHeaders(request.Header); err != nil {
		return err
	}
	PrepareRequest(request)
	return nil
}
