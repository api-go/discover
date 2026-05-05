package discover

import "net/http"

// SetRoute 设置全局路由规则，可以在请求前修改 App、Method、Path 等信息
func SetRoute(route func(appClient *AppClient, request *http.Request)) {
	settedRoute = route
}
