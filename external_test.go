package discover_test

import (
	"github.com/api-go/discover"
	gohttp "github.com/api-go/http"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDefaultExternalContextDoesNotChangeRPCTarget(t *testing.T) {
	discover.Init()
	old := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = old }()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Host") != "browser.example" || r.Header.Get("X-Scheme") != "https" || r.Header.Get("Origin") != "" || r.Header.Get("Access-Token") != "internal-token" {
			t.Error(r.Header)
		}
		if r.Host == "browser.example" || r.TLS != nil {
			t.Error("browser context replaced RPC target")
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	app := "external-context-test"
	discover.AddExternalApp(app, "1:internal-token")
	discover.SetNode(app, strings.TrimPrefix(server.URL, "http://"), 100)
	defer discover.SetNode(app, strings.TrimPrefix(server.URL, "http://"), 0)
	r := httptest.NewRequest("GET", "http://internal/", nil)
	r.RemoteAddr = "127.0.0.1:10"
	r.Header.Set("X-Host", "browser.example")
	r.Header.Set("X-Scheme", "https")
	r.Header.Set("Origin", "https://browser.example")
	r.Header.Set("X-Request-ID", "outer-gateway")
	resolver, _ := gohttp.NewExternalAddressResolver(gohttp.ExternalAddressPolicy{})
	if err := discover.PrepareExternalRequest(r, resolver); err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Host", "mutated")
	outgoing, _ := http.NewRequest("GET", "/test", nil)
	response, _, err := discover.NewCaller(r, nil).DoRequest(app, outgoing)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatal(response.StatusCode)
	}
}
