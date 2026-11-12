package discover

import (
	"net/http"
	"strings"
	"time"

	"github.com/api-go/log"
)

// CallLog is one compact outbound call summary. Phase=headers/handshake does
// not imply a streaming response or WebSocket has completed successfully.
type CallLog struct {
	log.BaseLog
	App      string  `log:"pos:6"`
	Target   string  `log:"pos:7,color:cyan"`
	Node     string  `log:"pos:8,color:gray"`
	Method   string  `log:"pos:9"`
	Path     string  `log:"pos:10,color:cyan"`
	Phase    string  `log:"pos:11"`
	Status   int     `log:"pos:12"`
	UsedTime float64 `log:"pos:13,precision:6"`
	Attempts int     `log:"pos:14"`
	Error    string  `log:"pos:15,color:red"`
}

func (l *CallLog) Reset() {
	l.BaseLog.Reset()
	l.App = ""
	l.Target = ""
	l.Node = ""
	l.Method = ""
	l.Path = ""
	l.Phase = ""
	l.Status = 0
	l.UsedTime = 0
	l.Attempts = 0
	l.Error = ""
}

func init() { log.RegisterType("call", &CallLog{}) }

func (c *Caller) callLog(start time.Time, app, method, path, phase, node string, response *http.Response, err error, attempts int) {
	if c.SuppressCallLog {
		return
	}
	logger := c.logger
	if logger == nil {
		logger = log.DefaultLogger
	}
	logger = logger.New(c.requestID)
	failed := err != nil || response != nil && response.StatusCode >= 400
	level := log.INFO
	if failed {
		level = log.WARNING
	}
	if !logger.CheckLevel(level) {
		return
	}
	entry := log.GetEntry[CallLog]()
	logger.FillBase(&entry.BaseLog, "call")
	appLock.RLock()
	entry.App = Config.App
	appLock.RUnlock()
	entry.Target = app
	entry.Node = node
	entry.Method = method
	entry.Path = strings.SplitN(path, "?", 2)[0]
	entry.Phase = phase
	entry.Attempts = attempts
	entry.UsedTime = time.Since(start).Seconds()
	if response != nil {
		entry.Status = response.StatusCode
	}
	if err != nil {
		entry.Error = err.Error()
	}
	logger.Log(entry)
}
