package discover_test

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/api-go/discover"
	"github.com/api-go/file"
	"github.com/api-go/log"
)

func TestCallLogPreservesTraceAndCanDelegateSummary(t *testing.T) {
	discover.Init()
	old := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = old }()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(discover.HeaderRequestId) != "trace-call-log" {
			t.Error("trace was not relayed")
		}
		w.WriteHeader(204)
	}))
	defer upstream.Close()
	app := "discover-call-log-test"
	addr := strings.TrimPrefix(upstream.URL, "http://")
	discover.AddExternalApp(app, "1")
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	path := filepath.Join(t.TempDir(), "call.log")
	logger := log.NewLogger(log.Config{File: path, NoCompress: true})
	req := httptest.NewRequest("GET", "http://entry/", nil)
	req.Header.Set(discover.HeaderRequestId, "trace-call-log")
	caller := discover.NewCaller(req, logger)
	res := caller.Get(app, "/endpoint?secret=never-log-this")
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	data, err := file.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(data, "trace-call-log") || !strings.Contains(data, "\"call\"") || strings.Contains(data, "never-log-this") {
		t.Fatal(data)
	}
	caller.SuppressCallLog = true
	if res = caller.Get(app, "/endpoint"); res.Error != nil {
		t.Fatal(res.Error)
	}
	after, err := file.Read(path)
	if err != nil || after != data {
		t.Fatalf("delegated call summary still logged: %v %s", err, after)
	}
	log.WriterService.Stop(nil)
}
