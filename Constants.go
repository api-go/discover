package discover

import (
	gohttp "apigo.cc/go/http"
)

const (
	HeaderFromApp  = gohttp.HeaderFromApp
	HeaderFromNode = gohttp.HeaderFromNode

	HeaderClientIP         = gohttp.HeaderClientIP
	HeaderForwardedFor     = gohttp.HeaderForwardedFor
	HeaderUserID           = gohttp.HeaderUserID
	HeaderDeviceID         = gohttp.HeaderDeviceID
	HeaderClientAppName    = gohttp.HeaderClientAppName
	HeaderClientAppVersion = gohttp.HeaderClientAppVersion
	HeaderSessionID        = gohttp.HeaderSessionID
	HeaderRequestID        = gohttp.HeaderRequestID
	HeaderHost             = gohttp.HeaderHost
	HeaderScheme           = gohttp.HeaderScheme
	HeaderUserAgent        = gohttp.HeaderUserAgent
)

var RelayHeaders = gohttp.RelayHeaders

const DefaultRegistry = "127.0.0.1:6379::15"
const EnvRegistry = "DISCOVER_REGISTRY"
const EnvApp = "DISCOVER_APP"
const EnvWeight = "DISCOVER_WEIGHT"
const EnvCalls = "DISCOVER_CALLS"
