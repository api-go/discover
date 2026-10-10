// A native net/http service used by the cross-language conformance runner.
package main

import (
	"apigo.cc/go/cast"
	"apigo.cc/go/discover"
	"fmt"
	"github.com/gorilla/websocket"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	discover.Init()
	discover.Config.App = os.Getenv("DISCOVER_TEST_APP")
	discover.Config.Registry = "redis://127.0.0.1:6379/15"
	discover.Config.Calls = map[string]string{}
	if !discover.Start(ln.Addr().String()) {
		panic("registration failed")
	}
	defer discover.Stop()
	mux := http.NewServeMux()
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		data, _ := cast.ToJSONBytes(map[string]any{"method": r.Method, "body": string(body), "headers": r.Header})
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})
	mux.HandleFunc("/fail", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		for {
			kind, data, err := connection.ReadMessage()
			if err != nil {
				return
			}
			if connection.WriteMessage(kind, data) != nil {
				return
			}
		}
	})
	server := &http.Server{Handler: h2c.NewHandler(discover.Middleware(mux), &http2.Server{})}
	go server.Serve(ln)
	fmt.Printf("DISCOVER_READY %s\n", ln.Addr())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
	<-stop
	server.Close()
}
