package main

import (
	"github.com/mszlu521/msgo"
	"github.com/mszlu521/msgo/gateway"
	"github.com/mszlu521/msgo/register"
	"net/http"
	"time"
)

func main() {
	engine := msgo.Default()
	engine.OpenGateway = true
	var configs []gateway.GWConfig
	configs = append(configs, gateway.GWConfig{
		Name: "order",
		Path: "/order/**",
		Header: func(req *http.Request) {
			req.Header.Set("my", "mszlu")
		},
		ServiceName: "orderCenter",
	}, gateway.GWConfig{
		Name: "goods",
		Path: "/goods/**",
		Header: func(req *http.Request) {
			req.Header.Set("my", "mszlu")
		},
		ServiceName: "goodsCenter",
	})
	engine.SetGatewayConfig(configs)
	engine.RegisterType = "etcd"
	engine.RegisterOption = register.Option{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	}
	engine.Run(":80")
}
