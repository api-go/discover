package discover

const (
	HeaderFromApp  = "X-Discover-From-App"
	HeaderFromNode = "X-Discover-From-Node"

	HeaderClientIp         = "X-Client-Ip"
	HeaderForwardedFor     = "X-Forwarded-For"
	HeaderUserId           = "X-User-Id"
	HeaderDeviceId         = "X-Device-Id"
	HeaderClientAppName    = "X-Client-App-Name"
	HeaderClientAppVersion = "X-Client-App-Version"
	HeaderSessionId        = "X-Session-Id"
	HeaderRequestId        = "X-Request-Id"
	HeaderHost             = "X-Host"
	HeaderScheme           = "X-Scheme"
	HeaderUserAgent        = "User-Agent"
)

var RelayHeaders = []string{
	HeaderClientIp,
	HeaderForwardedFor,
	HeaderUserId,
	HeaderDeviceId,
	HeaderClientAppName,
	HeaderClientAppVersion,
	HeaderSessionId,
	HeaderRequestId,
	HeaderHost,
	HeaderScheme,
	HeaderUserAgent,
}

const DefaultRegistry = "127.0.0.1:6379::15"
const EnvRegistry = "DISCOVER_REGISTRY"
const EnvApp = "DISCOVER_APP"
const EnvWeight = "DISCOVER_WEIGHT"
const EnvCalls = "DISCOVER_CALLS"
