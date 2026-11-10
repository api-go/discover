package discover_test

import (
	"github.com/api-go/discover"
	"github.com/api-go/log"
	"github.com/api-go/redis"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestHeartbeatStopWaitAndRestart(t *testing.T) {
	if os.Getenv("DISCOVER_REDIS_INTEGRATION") != "1" {
		t.Skip("set DISCOVER_REDIS_INTEGRATION=1")
	}
	rd := redis.GetRedis("127.0.0.1:6379", log.DefaultLogger)
	if rd.Error != nil {
		t.Fatal(rd.Error)
	}
	discover.Init()
	original := discover.Config
	app := fmt.Sprintf("discover-lifecycle-test-%d", time.Now().UnixNano())
	discover.Config.App = app
	discover.Config.Registry = "127.0.0.1:6379"
	discover.Config.Calls = map[string]string{}
	defer func() { discover.Stop(); discover.Config = original; rd.Do("DEL", app) }()
	for iteration := 0; iteration < 20; iteration++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 21000+iteration)
		if !discover.Start(addr) {
			t.Fatal("start failed")
		}
		if iteration%2 == 0 && !discover.Start(addr) {
			t.Fatal("restart failed")
		}
		awaitRegistered(t)
		if !rd.HEXISTS(app, addr) {
			t.Fatal("node not registered")
		}
		var waiters sync.WaitGroup
		for count := 0; count < 3; count++ {
			waiters.Add(1)
			go func() { defer waiters.Done(); discover.Wait() }()
		}
		discover.Stop()
		discover.Wait()
		completed := make(chan struct{})
		go func() { waiters.Wait(); close(completed) }()
		select {
		case <-completed:
		case <-time.After(time.Second):
			t.Fatal("Wait did not complete after Stop")
		}
		if rd.HEXISTS(app, addr) {
			t.Fatal("stopped node re-registered")
		}
	}
}
