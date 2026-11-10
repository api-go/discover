package main

import (
	"github.com/api-go/cast"
	"github.com/api-go/discover"
	"github.com/api-go/redis"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	app := os.Getenv("DISCOVER_TEST_APP")
	target := app + "-target"
	discover.Init()
	discover.Config.App = app
	discover.Config.Registry = os.Getenv("DISCOVER_TEST_REGISTRY")
	discover.Config.Calls = map[string]string{target: "1"}
	discover.Config.CallRetryTimes = 1
	var retained *bool
	before := time.Now()
	discover.Start("127.0.0.1:31000")
	started, _ := cast.ToJSON(map[string]any{"startedMs": float64(time.Since(before)) / float64(time.Millisecond)})
	fmt.Println(started)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-signals:
			discover.Stop()
			fmt.Println(`{"stopped":true}`)
			return
		case <-tick.C:
			if retained == nil && len(discover.GetAppNodes(target)) > 0 {
				result := discover.NewCaller(nil, nil).Get(target, "/")
				if result.Response != nil && result.Response.Body != nil {
					result.Response.Body.Close()
				}
				value := redis.GetRedis(discover.Config.Registry, nil).HEXISTS(target, "127.0.0.1:31999")
				retained = &value
			}
			status, _ := cast.ToJSON(map[string]any{"globalMembershipRetained": retained, "registered": discover.IsRegistered(), "nodes": len(discover.GetAppNodes(target))})
			fmt.Println(status)
		}
	}
}
