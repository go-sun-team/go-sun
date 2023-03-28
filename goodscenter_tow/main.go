package main

import (
	"encoding/gob"
	"github.com/mszlu521/goodscenter_two/model"
	"github.com/mszlu521/goodscenter_two/service"
	"github.com/mszlu521/msgo"
	"github.com/mszlu521/msgo/rpc"
	"log"
	"net/http"
)

func main() {
	engine := msgo.Default()

	group := engine.Group("goods")
	group.Get("/find", func(ctx *msgo.Context) {
		goods := &model.Goods{Id: 1000, Name: "9002的商品"}
		ctx.JSON(http.StatusOK, &model.Result{Code: 200, Msg: "success", Data: goods})
	})
	group.Post("/find", func(ctx *msgo.Context) {
		goods := &model.Goods{Id: 1000, Name: "9002的商品"}
		ctx.JSON(http.StatusOK, &model.Result{Code: 200, Msg: "success", Data: goods})
	})

	//server, _ := rpc.NewGrpcServer(":9111")
	//server.Register(func(g *grpc.Server) {
	//	api.RegisterGoodsApiServer(g, &api.GoodsRpcService{})
	//})
	//err := server.Run()
	//log.Println(err)
	tcpServer, err := rpc.NewTcpServer("127.0.0.1", 9223)
	log.Println(err)
	gob.Register(&model.Result{})
	gob.Register(&model.Goods{})
	tcpServer.Register("goods", &service.GoodsRpcService{})
	tcpServer.Run()
	engine.Run(":9005")
}
