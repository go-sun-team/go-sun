package main

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"github.com/mszlu521/msgo"
	"github.com/mszlu521/msgo/register"
	"github.com/mszlu521/msgo/rpc"
	"github.com/mszlu521/msgo/tracer"
	"github.com/mszlu521/ordercenter/api"
	"github.com/mszlu521/ordercenter/model"
	"github.com/mszlu521/ordercenter/service"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"log"
	"net/http"
	"time"
)

func main() {
	engine := msgo.Default()
	client := rpc.NewHttpClient()
	client.RegisterHttpService("goods", &service.GoodsService{})
	group := engine.Group("order")

	createTracer, closer, err := tracer.CreateTracer("orderCenter",
		&config.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		&config.ReporterConfig{
			LogSpans:          true,
			CollectorEndpoint: "http://192.168.200.100:14268/api/traces",
		}, config.Logger(jaeger.StdLogger),
	)
	if err != nil {
		log.Println(err)
	}
	defer closer.Close()

	group.Get("/find", func(ctx *msgo.Context) {
		//通过商品中心 查询商品的信息
		//http的方式进行调用
		params := make(map[string]any)
		params["id"] = ctx.GetQuery("id")
		params["name"] = "zhangsan"
		//body, err := client.PostJson("http://localhost:9002/goods/find", params)
		//if err != nil {
		//	panic(err)
		//}
		//log.Println(string(body))
		span := createTracer.StartSpan("find")
		defer span.Finish()
		session := client.Session()
		session.ReqHandler = func(req *http.Request) {
			ext.SpanKindRPCClient.Set(span)
			createTracer.Inject(span.Context(), opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))
		}
		body, err := session.Do("goods", "Find").(*service.GoodsService).Find(params)
		if err != nil {
			panic(err)
		}
		v := &model.Result{}
		json.Unmarshal(body, v)
		ctx.JSON(http.StatusOK, v)
	})

	group.Get("/findGrpc", func(ctx *msgo.Context) {
		config := rpc.DefaultGrpcClientConfig()
		config.Address = "localhost:9111"
		client, _ := rpc.NewGrpcClient(config)
		defer client.Conn.Close()
		goodsApiClient := api.NewGoodsApiClient(client.Conn)
		goodsResponse, _ := goodsApiClient.Find(context.Background(), &api.GoodsRequest{})
		ctx.JSON(http.StatusOK, goodsResponse)
	})

	group.Get("/findTcp", func(ctx *msgo.Context) {
		gob.Register(&model.Result{})
		gob.Register(&model.Goods{})
		option := rpc.DefaultOption
		option.SerializeType = rpc.ProtoBuff
		option.RegisterType = "etcd"
		option.RegisterOption = register.Option{
			Endpoints:   []string{"127.0.0.1:2379"},
			DialTimeout: 5 * time.Second,
		}
		proxy := rpc.NewMsTcpClientProxy(option)
		params := make([]any, 1)
		params[0] = int64(1)
		//var Find func(id int64) any 作业
		result, err := proxy.Call(context.Background(), "goods", "Find", params)
		//Find(1)
		log.Println(err)
		ctx.JSON(http.StatusOK, result)
	})

	engine.Run(":9003")
}
