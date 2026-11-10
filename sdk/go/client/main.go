package main

import (
	"github.com/api-go/discover"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	discover.Init()
	discover.Config.App = ""
	discover.Config.Registry = "redis://127.0.0.1:6379/15"
	app := os.Getenv("DISCOVER_TEST_TARGET")
	discover.Config.Calls = map[string]string{app: "1"}
	if !discover.Start("") {
		panic("start failed")
	}
	defer discover.Stop()
	timeout := time.NewTimer(8 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for len(discover.GetAppNodes(app)) == 0 {
		select {
		case <-timeout.C:
			panic("discovery timed out")
		case <-tick.C:
		}
	}
	request, _ := http.NewRequest("GET", discover.HealthPath, nil)
	response, _, err := discover.NewCaller(nil, nil).DoRequest(app, request)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 200 || string(body) != "OK" {
		panic(fmt.Sprintf("health: %d %q %v", response.StatusCode, body, err))
	}
	fmt.Println("PASS Go -> peer health")
}
