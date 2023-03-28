package rpc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mszlu521/msgo/register"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"io"
	"log"
	"net"
	"reflect"
	"sync/atomic"
	"time"
)

//TCP 客户端 服务端
//客户端 1. 连接服务端 2. 发送请求数据 （编码） 二进制 通过网络发送 3. 等待回复 接收到响应（解码）
//服务端 1. 启动服务 2. 接收请求 （解码），根据请求 调用对应的服务 得到响应数据 3. 将响应数据发送给客户端（编码）

type Serializer interface {
	Serialize(data any) ([]byte, error)
	DeSerialize(data []byte, target any) error
}

//Gob协议
type GobSerializer struct{}

func (c GobSerializer) Serialize(data any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(data); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (c GobSerializer) DeSerialize(data []byte, target any) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	return decoder.Decode(target)
}

type ProtobufSerializer struct{}

func (c ProtobufSerializer) Serialize(data any) ([]byte, error) {
	marshal, err := proto.Marshal(data.(proto.Message))
	if err != nil {
		return nil, err
	}
	return marshal, nil
}

func (c ProtobufSerializer) DeSerialize(data []byte, target any) error {
	message := target.(proto.Message)
	return proto.Unmarshal(data, message)
}

type SerializerType byte

const (
	Gob SerializerType = iota
	ProtoBuff
)

type CompressInterface interface {
	Compress([]byte) ([]byte, error)
	UnCompress([]byte) ([]byte, error)
}

type CompressType byte

const (
	Gzip CompressType = iota
)

type GzipCompress struct{}

func (c GzipCompress) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)

	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c GzipCompress) UnCompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	defer reader.Close()
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	// 从 Reader 中读取出数据
	if _, err := buf.ReadFrom(reader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

const MagicNumber byte = 0x1d
const Version = 0x01

type MessageType byte

const (
	msgRequest MessageType = iota
	msgResponse
	msgPing
	msgPong
)

type Header struct {
	MagicNumber   byte
	Version       byte
	FullLength    int32
	MessageType   MessageType
	CompressType  CompressType
	SerializeType SerializerType
	RequestId     int64
}

type MsRpcMessage struct {
	//头
	Header *Header
	//消息体
	Data any
}

type MsRpcRequest struct {
	RequestId   int64
	ServiceName string
	MethodName  string
	Args        []any
}

type MsRpcResponse struct {
	RequestId     int64
	Code          int16
	Msg           string
	CompressType  CompressType
	SerializeType SerializerType
	Data          any
}

type MsRpcServer interface {
	Register(name string, service interface{})
	Run()
	Stop()
}

type MsTcpServer struct {
	host           string
	port           int
	listen         net.Listener
	serviceMap     map[string]any
	RegisterType   string
	RegisterOption register.Option
	RegisterCli    register.MsRegister
	LimiterTimeOut time.Duration
	Limiter        *rate.Limiter
}

func NewTcpServer(host string, port int) (*MsTcpServer, error) {
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}
	m := &MsTcpServer{serviceMap: make(map[string]any)}
	m.listen = listen
	m.port = port
	m.host = host
	return m, nil
}
func (s *MsTcpServer) SetLimiter(limit, cap int) {
	s.Limiter = rate.NewLimiter(rate.Limit(limit), cap)
}
func (s *MsTcpServer) Register(name string, service interface{}) {
	t := reflect.TypeOf(service)
	if t.Kind() != reflect.Pointer {
		panic("service must be pointer")
	}
	s.serviceMap[name] = service

	err := s.RegisterCli.CreateCli(s.RegisterOption)
	if err != nil {
		panic(err)
	}
	err = s.RegisterCli.RegisterService(name, s.host, s.port)
	if err != nil {
		panic(err)
	}
}

type MsTcpConn struct {
	conn    net.Conn
	rspChan chan *MsRpcResponse
}

func (c MsTcpConn) Send(rsp *MsRpcResponse) error {
	if rsp.Code != 200 {
		//进行默认的数据发送
	}
	//编码 发送出去
	headers := make([]byte, 17)
	//magic number
	headers[0] = MagicNumber
	//version
	headers[1] = Version
	//full length
	//消息类型
	headers[6] = byte(msgResponse)
	//压缩类型
	headers[7] = byte(rsp.CompressType)
	//序列化
	headers[8] = byte(rsp.SerializeType)
	//请求id
	binary.BigEndian.PutUint64(headers[9:], uint64(rsp.RequestId))
	//编码 先序列化 在压缩
	se := loadSerializer(rsp.SerializeType)
	var body []byte
	var err error
	if rsp.SerializeType == ProtoBuff {
		pRsp := &Response{}
		pRsp.SerializeType = int32(rsp.SerializeType)
		pRsp.CompressType = int32(rsp.CompressType)
		pRsp.Code = int32(rsp.Code)
		pRsp.Msg = rsp.Msg
		pRsp.RequestId = rsp.RequestId
		//value, err := structpb.
		//	log.Println(err)
		m := make(map[string]any)
		marshal, _ := json.Marshal(rsp.Data)
		_ = json.Unmarshal(marshal, &m)
		value, err := structpb.NewStruct(m)
		log.Println(err)
		pRsp.Data = structpb.NewStructValue(value)
		body, err = se.Serialize(pRsp)
	} else {
		body, err = se.Serialize(rsp)
	}
	if err != nil {
		return err
	}
	com := loadCompress(rsp.CompressType)
	body, err = com.Compress(body)
	if err != nil {
		return err
	}
	fullLen := 17 + len(body)
	binary.BigEndian.PutUint32(headers[2:6], uint32(fullLen))

	_, err = c.conn.Write(headers[:])
	if err != nil {
		return err
	}
	_, err = c.conn.Write(body[:])
	if err != nil {
		return err
	}
	return nil
}

func (s *MsTcpServer) Stop() {
	err := s.listen.Close()
	if err != nil {
		log.Println(err)
	}
}

func (s *MsTcpServer) Run() {
	for {
		conn, err := s.listen.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		msConn := &MsTcpConn{conn: conn, rspChan: make(chan *MsRpcResponse, 1)}
		//1. 一直接收数据 解码工作 请求业务获取结果 发送到rspChan
		//2. 获得结果 编码 发送数据
		go s.readHandle(msConn)
		go s.writeHandle(msConn)
	}
}

func (s *MsTcpServer) readHandle(conn *MsTcpConn) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("readHandle recover ", err)
			conn.conn.Close()
		}
	}()
	//在这加一个限流
	ctx, cancel := context.WithTimeout(context.Background(), s.LimiterTimeOut)
	defer cancel()
	err2 := s.Limiter.WaitN(ctx, 1)
	if err2 != nil {
		rsp := &MsRpcResponse{}
		rsp.Code = 700 //被限流的错误
		rsp.Msg = err2.Error()
		conn.rspChan <- rsp
		return
	}
	//接收数据
	//解码
	msg, err := decodeFrame(conn.conn)
	if err != nil {
		rsp := &MsRpcResponse{}
		rsp.Code = 500
		rsp.Msg = err.Error()
		conn.rspChan <- rsp
		return
	}
	if msg.Header.MessageType == msgRequest {
		if msg.Header.SerializeType == ProtoBuff {
			req := msg.Data.(*Request)
			rsp := &MsRpcResponse{RequestId: req.RequestId}
			rsp.SerializeType = msg.Header.SerializeType
			rsp.CompressType = msg.Header.CompressType
			serviceName := req.ServiceName
			service, ok := s.serviceMap[serviceName]
			if !ok {
				rsp := &MsRpcResponse{}
				rsp.Code = 500
				rsp.Msg = errors.New("no service found").Error()
				conn.rspChan <- rsp
				return
			}
			methodName := req.MethodName
			method := reflect.ValueOf(service).MethodByName(methodName)
			if method.IsNil() {
				rsp := &MsRpcResponse{}
				rsp.Code = 500
				rsp.Msg = errors.New("no service method found").Error()
				conn.rspChan <- rsp
				return
			}
			//调用方法
			args := make([]reflect.Value, len(req.Args))
			for i := range req.Args {
				of := reflect.ValueOf(req.Args[i].AsInterface())
				of = of.Convert(method.Type().In(i))
				args[i] = of
			}
			result := method.Call(args)

			results := make([]any, len(result))
			for i, v := range result {
				results[i] = v.Interface()
			}
			err, ok := results[len(result)-1].(error)
			if ok {
				rsp.Code = 500
				rsp.Msg = err.Error()
				conn.rspChan <- rsp
				return
			}
			rsp.Code = 200
			rsp.Data = results[0]
			conn.rspChan <- rsp
		} else {
			req := msg.Data.(*MsRpcRequest)
			rsp := &MsRpcResponse{RequestId: req.RequestId}
			rsp.SerializeType = msg.Header.SerializeType
			rsp.CompressType = msg.Header.CompressType
			serviceName := req.ServiceName
			service, ok := s.serviceMap[serviceName]
			if !ok {
				rsp := &MsRpcResponse{}
				rsp.Code = 500
				rsp.Msg = errors.New("no service found").Error()
				conn.rspChan <- rsp
				return
			}
			methodName := req.MethodName
			method := reflect.ValueOf(service).MethodByName(methodName)
			if method.IsNil() {
				rsp := &MsRpcResponse{}
				rsp.Code = 500
				rsp.Msg = errors.New("no service method found").Error()
				conn.rspChan <- rsp
				return
			}
			//调用方法
			args := req.Args
			var valuesArg []reflect.Value
			for _, v := range args {
				valuesArg = append(valuesArg, reflect.ValueOf(v))
			}
			result := method.Call(valuesArg)

			results := make([]any, len(result))
			for i, v := range result {
				results[i] = v.Interface()
			}
			err, ok := results[len(result)-1].(error)
			if ok {
				rsp.Code = 500
				rsp.Msg = err.Error()
				conn.rspChan <- rsp
				return
			}
			rsp.Code = 200
			rsp.Data = results[0]
			conn.rspChan <- rsp
		}
	}
}

func (s *MsTcpServer) writeHandle(conn *MsTcpConn) {
	select {
	case rsp := <-conn.rspChan:
		defer conn.conn.Close()
		//发送数据
		err := conn.Send(rsp)
		if err != nil {
			log.Println(err)
		}

	}
}

func (s *MsTcpServer) SetRegister(registerType string, option register.Option) {
	s.RegisterType = registerType
	s.RegisterOption = option
	if registerType == "nacos" {
		s.RegisterCli = &register.MsNacosRegister{}
	}
	if registerType == "etcd" {
		s.RegisterCli = &register.MsEtcdRegister{}
	}
}

func decodeFrame(conn net.Conn) (*MsRpcMessage, error) {
	//1+1+4+1+1+1+8=17
	headers := make([]byte, 17)
	_, err := io.ReadFull(conn, headers)
	if err != nil {
		return nil, err
	}
	mn := headers[0]
	if mn != MagicNumber {
		return nil, errors.New("magic number error")
	}
	//version
	vs := headers[1]
	//full length
	//网络传输 大端
	fullLength := int32(binary.BigEndian.Uint32(headers[2:6]))
	//messageType
	messageType := headers[6]
	//压缩类型
	compressType := headers[7]
	//序列化类型
	seType := headers[8]
	//请求id
	requestId := int64(binary.BigEndian.Uint32(headers[9:]))

	msg := &MsRpcMessage{
		Header: &Header{},
	}
	msg.Header.MagicNumber = mn
	msg.Header.Version = vs
	msg.Header.FullLength = fullLength
	msg.Header.MessageType = MessageType(messageType)
	msg.Header.CompressType = CompressType(compressType)
	msg.Header.SerializeType = SerializerType(seType)
	msg.Header.RequestId = requestId

	//body
	bodyLen := fullLength - 17
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(conn, body)
	if err != nil {
		return nil, err
	}
	//编码的 先序列化 后 压缩
	//解码的时候 先解压缩，反序列化
	compress := loadCompress(CompressType(compressType))
	if compress == nil {
		return nil, errors.New("no compress")
	}
	body, err = compress.UnCompress(body)
	if compress == nil {
		return nil, err
	}
	serializer := loadSerializer(SerializerType(seType))
	if serializer == nil {
		return nil, errors.New("no serializer")
	}
	if MessageType(messageType) == msgRequest {
		if SerializerType(seType) == ProtoBuff {
			req := &Request{}
			err := serializer.DeSerialize(body, req)
			if err != nil {
				return nil, err
			}
			msg.Data = req
		} else {
			req := &MsRpcRequest{}
			err := serializer.DeSerialize(body, req)
			if err != nil {
				return nil, err
			}
			msg.Data = req
		}
		return msg, nil
	}
	if MessageType(messageType) == msgResponse {
		if SerializerType(seType) == ProtoBuff {
			rsp := &Response{}
			err := serializer.DeSerialize(body, rsp)
			if err != nil {
				return nil, err
			}
			msg.Data = rsp
		} else {
			rsp := &MsRpcResponse{}
			err := serializer.DeSerialize(body, rsp)
			if err != nil {
				return nil, err
			}
			msg.Data = rsp
		}

		return msg, nil
	}
	return nil, errors.New("no message type")
}

func loadSerializer(serializerType SerializerType) Serializer {
	switch serializerType {
	case Gob:
		return GobSerializer{}
	case ProtoBuff:
		return ProtobufSerializer{}
	}
	return nil
}

func loadCompress(compressType CompressType) CompressInterface {
	switch compressType {
	case Gzip:
		return GzipCompress{}
	}
	return nil
}

type MsRpcClient interface {
	Connect() error
	Invoke(context context.Context, serviceName string, methodName string, args []any) (any, error)
	Close() error
}

type MsTcpClient struct {
	conn        net.Conn
	option      TcpClientOption
	ServiceName string
	RegisterCli register.MsRegister
}
type TcpClientOption struct {
	Retries           int
	ConnectionTimeout time.Duration
	SerializeType     SerializerType
	CompressType      CompressType
	Host              string
	Port              int
	RegisterType      string
	RegisterOption    register.Option
	RegisterCli       register.MsRegister
}

var DefaultOption = TcpClientOption{
	Host:              "127.0.0.1",
	Port:              9222,
	Retries:           3,
	ConnectionTimeout: 5 * time.Second,
	SerializeType:     Gob,
	CompressType:      Gzip,
}

func NewTcpClient(option TcpClientOption) *MsTcpClient {
	return &MsTcpClient{option: option}
}

func (c *MsTcpClient) Connect() error {
	var addr string
	err := c.RegisterCli.CreateCli(c.option.RegisterOption)
	if err != nil {
		panic(err)
	}
	addr, err = c.RegisterCli.GetValue(c.ServiceName)
	if err != nil {
		panic(err)
	}
	conn, err := net.DialTimeout("tcp", addr, c.option.ConnectionTimeout)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *MsTcpClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

var reqId int64

func (c *MsTcpClient) Invoke(ctx context.Context, serviceName string, methodName string, args []any) (any, error) {
	//包装 request对象 编码 发送即可
	req := &MsRpcRequest{}
	req.RequestId = atomic.AddInt64(&reqId, 1)
	req.ServiceName = serviceName
	req.MethodName = methodName
	req.Args = args

	headers := make([]byte, 17)
	//magic number
	headers[0] = MagicNumber
	//version
	headers[1] = Version
	//full length
	//消息类型
	headers[6] = byte(msgRequest)
	//压缩类型
	headers[7] = byte(c.option.CompressType)
	//序列化
	headers[8] = byte(c.option.SerializeType)
	//请求id
	binary.BigEndian.PutUint64(headers[9:], uint64(req.RequestId))

	serializer := loadSerializer(c.option.SerializeType)
	if serializer == nil {
		return nil, errors.New("no serializer")
	}
	var body []byte
	var err error
	if c.option.SerializeType == ProtoBuff {
		pReq := &Request{}
		pReq.RequestId = atomic.AddInt64(&reqId, 1)
		pReq.ServiceName = serviceName
		pReq.MethodName = methodName
		listValue, err := structpb.NewList(args)
		if err != nil {
			return nil, err
		}
		pReq.Args = listValue.Values
		body, err = serializer.Serialize(pReq)
	} else {
		body, err = serializer.Serialize(req)
	}

	if err != nil {
		return nil, err
	}
	compress := loadCompress(c.option.CompressType)
	if compress == nil {
		return nil, errors.New("no compress")
	}
	body, err = compress.Compress(body)
	if err != nil {
		return nil, err
	}
	fullLen := 17 + len(body)
	binary.BigEndian.PutUint32(headers[2:6], uint32(fullLen))

	_, err = c.conn.Write(headers[:])
	if err != nil {
		return nil, err
	}

	_, err = c.conn.Write(body[:])
	if err != nil {
		return nil, err
	}
	rspChan := make(chan *MsRpcResponse)
	go c.readHandle(rspChan)
	rsp := <-rspChan
	return rsp, nil
}

func (c *MsTcpClient) readHandle(rspChan chan *MsRpcResponse) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("MsTcpClient readHandle recover: ", err)
			c.conn.Close()
		}
	}()
	for {
		msg, err := decodeFrame(c.conn)
		if err != nil {
			log.Println("未解析出任何数据")
			rsp := &MsRpcResponse{}
			rsp.Code = 500
			rsp.Msg = err.Error()
			rspChan <- rsp
			return
		}
		//根据请求
		if msg.Header.MessageType == msgResponse {
			if msg.Header.SerializeType == ProtoBuff {
				rsp := msg.Data.(*Response)
				asInterface := rsp.Data.AsInterface()
				marshal, _ := json.Marshal(asInterface)
				rsp1 := &MsRpcResponse{}
				json.Unmarshal(marshal, rsp1)
				rspChan <- rsp1
			} else {
				rsp := msg.Data.(*MsRpcResponse)
				rspChan <- rsp
			}
			return
		}
	}
}

func (c *MsTcpClient) decodeFrame(conn net.Conn) (*MsRpcMessage, error) {
	//1+1+4+1+1+1+8=17
	headers := make([]byte, 17)
	_, err := io.ReadFull(conn, headers)
	if err != nil {
		return nil, err
	}
	mn := headers[0]
	if mn != MagicNumber {
		return nil, errors.New("magic number error")
	}
	//version
	vs := headers[1]
	//full length
	//网络传输 大端
	fullLength := int32(binary.BigEndian.Uint32(headers[2:6]))
	//messageType
	messageType := headers[6]
	//压缩类型
	compressType := headers[7]
	//序列化类型
	seType := headers[8]
	//请求id
	requestId := int64(binary.BigEndian.Uint32(headers[9:]))

	msg := &MsRpcMessage{
		Header: &Header{},
	}
	msg.Header.MagicNumber = mn
	msg.Header.Version = vs
	msg.Header.FullLength = fullLength
	msg.Header.MessageType = MessageType(messageType)
	msg.Header.CompressType = CompressType(compressType)
	msg.Header.SerializeType = SerializerType(seType)
	msg.Header.RequestId = requestId

	//body
	bodyLen := fullLength - 17
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(conn, body)
	if err != nil {
		return nil, err
	}
	//编码的 先序列化 后 压缩
	//解码的时候 先解压缩，反序列化
	compress := loadCompress(CompressType(compressType))
	if compress == nil {
		return nil, errors.New("no compress")
	}
	body, err = compress.UnCompress(body)
	if compress == nil {
		return nil, err
	}
	serializer := loadSerializer(SerializerType(seType))
	if serializer == nil {
		return nil, errors.New("no serializer")
	}
	if MessageType(messageType) == msgRequest {
		req := &MsRpcRequest{}
		err := serializer.DeSerialize(body, req)
		if err != nil {
			return nil, err
		}
		msg.Data = req
		return msg, nil
	}
	if MessageType(messageType) == msgResponse {
		rsp := &MsRpcResponse{}
		err := serializer.DeSerialize(body, rsp)
		if err != nil {
			return nil, err
		}
		msg.Data = rsp
		return msg, nil
	}
	return nil, errors.New("no message type")
}

type MsTcpClientProxy struct {
	client *MsTcpClient
	option TcpClientOption
}

func NewMsTcpClientProxy(option TcpClientOption) *MsTcpClientProxy {
	return &MsTcpClientProxy{option: option}
}
func (p *MsTcpClientProxy) Call(ctx context.Context, serviceName string, methodName string, args []any) (any, error) {
	client := NewTcpClient(p.option)
	client.ServiceName = serviceName
	if p.option.RegisterType == "nacos" {
		client.RegisterCli = &register.MsNacosRegister{}
	}
	if p.option.RegisterType == "etcd" {
		client.RegisterCli = &register.MsEtcdRegister{}
	}
	p.client = client
	err := client.Connect()
	if err != nil {
		return nil, err
	}
	for i := 0; i < p.option.Retries; i++ {
		result, err := client.Invoke(ctx, serviceName, methodName, args)
		if err != nil {
			if i >= p.option.Retries-1 {
				log.Println(errors.New("already retry all time"))
				client.Close()
				return nil, err
			}
			//睡眠一小会
			continue
		}
		client.Close()
		return result, nil
	}
	return nil, errors.New("retry time is 0")
}
