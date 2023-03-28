package service

import "github.com/mszlu521/msgo/rpc"

type GoodsService struct {
	Find func(args map[string]any) ([]byte, error) `msrpc:"GET,/goods/find"`
}

func (*GoodsService) Env() rpc.HttpConfig {
	return rpc.HttpConfig{
		Host: "localhost",
		Port: 9002,
	}
}
