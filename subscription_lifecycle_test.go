package discover_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"apigo.cc/go/discover"
)

func TestDynamicSubscriptionsDuringRestart(t *testing.T) {
	if os.Getenv("DISCOVER_REDIS_INTEGRATION") != "1" {
		t.Skip("set DISCOVER_REDIS_INTEGRATION=1")
	}
	discover.Init()
	original := discover.Config
	prefix := fmt.Sprintf("discover-subscription-test-%d", time.Now().UnixNano())
	discover.Config.App = prefix
	discover.Config.Registry = "redis://127.0.0.1:6379/15"
	discover.Config.Calls = map[string]string{}
	defer func() { discover.Stop(); discover.Config = original }()
	for iteration := range 4 {
		if !discover.Start("127.0.0.1:21999") {
			t.Fatal("start failed")
		}
		var workers sync.WaitGroup
		for index := range 8 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				discover.AddExternalApp(fmt.Sprintf("%s-%d-%d", prefix, iteration, index), "2")
			}()
		}
		workers.Add(1)
		go func() { defer workers.Done(); discover.Stop() }()
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("dynamic subscriptions or shutdown did not finish")
		}
		discover.Stop()
	}
}
