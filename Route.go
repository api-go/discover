package discover

import "net/http"

// SetRoute 设置路由规则
func (d *Discoverer) SetRoute(route func(appClient *AppClient, request *http.Request)) {
	d.settedRoute = route
}
