package main

import (
	"errors"
	"github.com/mszlu521/goodscenter/model"
	"github.com/mszlu521/msgo"
	"github.com/mszlu521/msgo/breaker"
	"github.com/mszlu521/msgo/tracer"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"log"
	"net/http"
)

func main() {
	engine := msgo.Default()

	engine.Use(msgo.Tracer("goodsService", &config.SamplerConfig{
		Type:  jaeger.SamplerTypeConst,
		Param: 1,
	}, &config.ReporterConfig{
		LogSpans:          true,
		CollectorEndpoint: "http://192.168.200.100:14268/api/traces",
	}, config.Logger(jaeger.StdLogger)))

	//engine.Use(msgo.Limiter(1, 1))
	group := engine.Group("goods")
	settings := breaker.Settings{}
	settings.Fallback = func(err error) (any, error) {
		goods := &model.Goods{Id: 1000, Name: "这是降级的商品"}
		return goods, nil
	}
	var cb = breaker.NewCircuitBreaker(settings)
	createTracer, closer, err := tracer.CreateTracer("goodsCenter", &config.SamplerConfig{
		Type:  jaeger.SamplerTypeConst,
		Param: 1,
	}, &config.ReporterConfig{
		LogSpans:          true,
		CollectorEndpoint: "http://192.168.200.100:14268/api/traces",
	}, config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(err)
	}
	defer closer.Close()
	group.Get("/findTracer", func(ctx *msgo.Context) {
		span := createTracer.StartSpan("findTracer")
		defer span.Finish()
		B(createTracer, span)
		goods := &model.Goods{Id: 1000, Name: "9002的商品"}
		ctx.JSON(http.StatusOK, &model.Result{Code: 200, Msg: "success", Data: goods})
	})

	group.Get("/find", func(ctx *msgo.Context) {
		result, err := cb.Execute(func() (any, error) {
			query := ctx.GetQuery("id")
			if query == "2" {
				return nil, errors.New("测试熔断")
			}
			goods := &model.Goods{Id: 1000, Name: "9002的商品"}
			return goods, nil
		})
		log.Println(err)
		//if err != nil {
		//	ctx.JSON(http.StatusInternalServerError, &model.Result{Code: 500, Msg: err.Error()})
		//	return
		//}
		ctx.JSON(http.StatusOK, &model.Result{Code: 200, Msg: "success", Data: result})
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
	//tcpServer, err := rpc.NewTcpServer("127.0.0.1", 9222)
	//tcpServer.SetRegister("etcd", register.Option{
	//	Endpoints:   []string{"127.0.0.1:2379"},
	//	DialTimeout: 5 * time.Second,
	//	Host:        "127.0.0.1",
	//	Port:        9222,
	//})
	//log.Println(err)
	//gob.Register(&model.Result{})
	//gob.Register(&model.Goods{})
	//tcpServer.Register("goods", &service.GoodsRpcService{})
	//tcpServer.LimiterTimeOut = time.Second
	//tcpServer.SetLimiter(10, 100)
	//tcpServer.Run()
	//cli := register.MsEtcdRegister{}
	//cli.CreateCli(register.Option{
	//	Endpoints:   []string{"127.0.0.1:2379"},
	//	DialTimeout: 5 * time.Second,
	//})
	//cli.RegisterService("goodsCenter", "127.0.0.1", 9002)
	engine.Run(":9002")

}

func B(createTracer opentracing.Tracer, span opentracing.Span) {
	log.Println("调用了一个B方法")
	startSpan := createTracer.StartSpan("B", opentracing.ChildOf(span.Context()))
	defer startSpan.Finish()
}
