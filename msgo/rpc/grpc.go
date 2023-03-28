package rpc

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"net"
	"time"
)

//listen, _ := net.Listen("tcp", ":9111")
//	server := grpc.NewServer()
//	api.RegisterGoodsApiServer(server, &api.GoodsRpcService{})
//	err := server.Serve(listen)

type MsGrpcServer struct {
	listen   net.Listener
	g        *grpc.Server
	register []func(g *grpc.Server)
	ops      []grpc.ServerOption
}

func NewGrpcServer(addr string, ops ...MsGrpcOption) (*MsGrpcServer, error) {
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	ms := &MsGrpcServer{}
	ms.listen = listen
	for _, v := range ops {
		v.Apply(ms)
	}
	server := grpc.NewServer(ms.ops...)
	ms.g = server
	return ms, nil
}

func (s *MsGrpcServer) Run() error {
	for _, f := range s.register {
		f(s.g)
	}
	return s.g.Serve(s.listen)
}

func (s *MsGrpcServer) Stop() {
	s.g.Stop()
}

func (s *MsGrpcServer) Register(f func(g *grpc.Server)) {
	s.register = append(s.register, f)
}

type MsGrpcOption interface {
	Apply(s *MsGrpcServer)
}

type DefaultMsGrpcOption struct {
	f func(s *MsGrpcServer)
}

func (d *DefaultMsGrpcOption) Apply(s *MsGrpcServer) {
	d.f(s)
}

func WithGrpcOptions(ops ...grpc.ServerOption) MsGrpcOption {
	return &DefaultMsGrpcOption{
		f: func(s *MsGrpcServer) {
			s.ops = append(s.ops, ops...)
		},
	}
}

type MsGrpcClient struct {
	Conn *grpc.ClientConn
}

func NewGrpcClient(config *MsGrpcClientConfig) (*MsGrpcClient, error) {
	var ctx = context.Background()
	var dialOptions = config.dialOptions

	if config.Block {
		//阻塞
		if config.DialTimeout > time.Duration(0) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, config.DialTimeout)
			defer cancel()
		}
		dialOptions = append(dialOptions, grpc.WithBlock())
	}
	if config.KeepAlive != nil {
		dialOptions = append(dialOptions, grpc.WithKeepaliveParams(*config.KeepAlive))
	}
	conn, err := grpc.DialContext(ctx, config.Address, dialOptions...)
	if err != nil {
		return nil, err
	}
	return &MsGrpcClient{
		Conn: conn,
	}, nil
}

type MsGrpcClientConfig struct {
	Address     string
	Block       bool
	DialTimeout time.Duration
	ReadTimeout time.Duration
	Direct      bool
	KeepAlive   *keepalive.ClientParameters
	dialOptions []grpc.DialOption
}

func DefaultGrpcClientConfig() *MsGrpcClientConfig {
	return &MsGrpcClientConfig{
		dialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		DialTimeout: time.Second * 3,
		ReadTimeout: time.Second * 2,
		Block:       true,
	}
}
