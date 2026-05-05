package discover

import (
	"apigo.cc/go/log"
)

const LogTypeDiscover = "discover"

type DiscoverLog struct {
	log.BaseLog
	App      string  `log:"pos:9,color:cyan"`
	Method   string  `log:"pos:10,color:magenta"`
	Path     string  `log:"pos:11,color:blue"`
	Node     string  `log:"pos:12,color:yellow"`
	Attempts int     `log:"pos:13"`
	UsedTime float32 `log:"pos:14,format:%.2fms"`
	Error    string  `log:"pos:15,color:red"`
}

func init() {
	log.RegisterType(LogTypeDiscover, DiscoverLog{})
}

func (ac *AppClient) Log(node string, usedTime float32, err error) {
	if ac.Logger == nil {
		ac.Logger = log.DefaultLogger
	}
	if !ac.Logger.CheckLevel(log.INFO) {
		return
	}

	entry := log.GetEntry[DiscoverLog]()
	// 框架会自动调用 fillBase，只需填充业务字段
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
