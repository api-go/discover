package discover

import (
	"apigo.cc/go/log"
)

const LogTypeDiscover = "discover"

type DiscoverLog struct {
	log.BaseLog
	App      string
	Method   string
	Path     string
	Node     string
	Attempts int
	UsedTime float32
	Error    string
}

func (ac *AppClient) Log(node string, usedTime float32, err error) {
	if ac.Logger == nil {
		ac.Logger = log.DefaultLogger
	}
	if !ac.Logger.CheckLevel(log.INFO) {
		return
	}

	entry := log.GetEntry[DiscoverLog]()
	// 框架会自动调用 FillBase，不再手动调用
	entry.App = ac.App
	entry.Method = ac.Method
	entry.Path = ac.Path
	entry.Node = node
	entry.Attempts = ac.attempts
	entry.UsedTime = usedTime
	if err != nil {
		entry.Error = err.Error()
	}
	ac.Logger.Log(entry)
}
