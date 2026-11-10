package discover

import (
	gohttp "github.com/api-go/http"
	"github.com/api-go/id"
	"net"
	"net/http"
	"sync"
)

const HealthPath = "/_discover/health"

var healthLock sync.RWMutex
var healthHandler http.Handler

// SetHealthHandler overrides the health response. nil restores the default.
// The application owns any dependency checks and response semantics.
func SetHealthHandler(handler http.Handler) {
	healthLock.Lock()
	healthHandler = handler
	healthLock.Unlock()
}

// HealthHandler returns 503 while a configured server has no confirmed registration.
// Custom handlers override this policy. It performs no synchronous downstream probes.
func HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		healthLock.RLock()
		handler := healthHandler
		healthLock.RUnlock()
		if handler != nil {
			handler.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		appLock.RLock()
		unavailable := isServer && !registered
		appLock.RUnlock()
		if unavailable {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("Discover registration unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
}

// Middleware mounts the health path and fills missing request metadata.
// External ingress must resolve trusted proxy/client identity before this middleware.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == HealthPath {
			HealthHandler().ServeHTTP(w, r)
			return
		}
		PrepareRequest(r)
		next.ServeHTTP(w, r)
	})
}

// PrepareRequest fills missing metadata without mounting a handler or reading a body.
// Frameworks should first resolve client IP using their own ingress trust policy.
func PrepareRequest(request *http.Request) {
	if request == nil {
		return
	}
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	if request.Header.Get(HeaderRequestId) == "" {
		request.Header.Set(HeaderRequestId, id.MakeID(32))
	}
	if request.Header.Get(HeaderClientIp) == "" {
		ip, _, err := net.SplitHostPort(request.RemoteAddr)
		if err != nil {
			ip = request.RemoteAddr
		}
		if net.ParseIP(ip) != nil {
			request.Header.Set(HeaderClientIp, ip)
		}
	}
	if address, prepared := gohttp.PreparedExternalAddress(request); prepared {
		_ = address.RelayHeaders(request.Header)
	} else {
		if request.Header.Get(HeaderHost) == "" && request.Host != "" {
			request.Header.Set(HeaderHost, request.Host)
		}
		if request.Header.Get(HeaderScheme) == "" {
			scheme := "http"
			if request.TLS != nil {
				scheme = "https"
			}
			request.Header.Set(HeaderScheme, scheme)
		}
	}
	if request.Header.Get(HeaderUserAgent) == "" && request.UserAgent() != "" {
		request.Header.Set(HeaderUserAgent, request.UserAgent())
	}
}
