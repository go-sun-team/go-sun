package msgo

import (
	"fmt"
	"github.com/mszlu521/msgo/config"
	"github.com/mszlu521/msgo/gateway"
	msLog "github.com/mszlu521/msgo/log"
	"github.com/mszlu521/msgo/register"
	"github.com/mszlu521/msgo/render"
	"html/template"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
)

const ANY = "ANY"

type HandlerFunc func(ctx *Context)

type MiddlewareFunc func(handlerFunc HandlerFunc) HandlerFunc

type routerGroup struct {
	name               string
	handleFuncMap      map[string]map[string]HandlerFunc
	middlewaresFuncMap map[string]map[string][]MiddlewareFunc
	handlerMethodMap   map[string][]string
	treeNode           *treeNode
	middlewares        []MiddlewareFunc
}

func (r *routerGroup) Use(middlewareFunc ...MiddlewareFunc) {
	r.middlewares = append(r.middlewares, middlewareFunc...)
}

func (r *routerGroup) methodHandle(name string, method string, h HandlerFunc, ctx *Context) {
	//组通用中间件
	if r.middlewares != nil {
		for _, middlewareFunc := range r.middlewares {
			h = middlewareFunc(h)
		}
	}
	//组路由级别
	middlewareFuncs := r.middlewaresFuncMap[name][method]
	if middlewareFuncs != nil {
		for _, middlewareFunc := range middlewareFuncs {
			h = middlewareFunc(h)
		}
	}
	h(ctx)
}

//func (r *routerGroup) Add(Name string, handleFunc HandlerFunc) {
//	r.handleFuncMap[Name] = handleFunc
//}

func (r *routerGroup) handle(name string, method string, handlerFunc HandlerFunc, middlewareFunc ...MiddlewareFunc) {
	_, ok := r.handleFuncMap[name]
	if !ok {
		r.handleFuncMap[name] = make(map[string]HandlerFunc)
		r.middlewaresFuncMap[name] = make(map[string][]MiddlewareFunc)
	}
	_, ok = r.handleFuncMap[name][method]
	if ok {
		panic("有重复的路由")
	}
	r.handleFuncMap[name][method] = handlerFunc
	r.middlewaresFuncMap[name][method] = append(r.middlewaresFuncMap[name][method], middlewareFunc...)
	r.treeNode.Put(name)
}

func (r *routerGroup) Any(name string, handlerFunc HandlerFunc, middlewareFunc ...MiddlewareFunc) {
	r.handle(name, ANY, handlerFunc, middlewareFunc...)
}

func (r *routerGroup) Get(name string, handlerFunc HandlerFunc, middlewareFunc ...MiddlewareFunc) {
	r.handle(name, http.MethodGet, handlerFunc, middlewareFunc...)
}
func (r *routerGroup) Post(name string, handlerFunc HandlerFunc, middlewareFunc ...MiddlewareFunc) {
	r.handle(name, http.MethodPost, handlerFunc, middlewareFunc...)
}

func (r *routerGroup) Delete(name string, handlerFunc HandlerFunc, middlewareFunc ...MiddlewareFunc) {
	r.handle(name, http.MethodDelete, handlerFunc, middlewareFunc...)
}
func (r *routerGroup) Put(name string, handlerFunc HandlerFunc, middlewareFunc ...MiddlewareFunc) {
	r.handle(name, http.MethodPut, handlerFunc, middlewareFunc...)
}
func (r *routerGroup) Patch(name string, handlerFunc HandlerFunc, middlewareFunc ...MiddlewareFunc) {
	r.handle(name, http.MethodPatch, handlerFunc, middlewareFunc...)
}
func (r *routerGroup) Options(name string, handlerFunc HandlerFunc, middlewareFunc ...MiddlewareFunc) {
	r.handle(name, http.MethodOptions, handlerFunc, middlewareFunc...)
}
func (r *routerGroup) Head(name string, handlerFunc HandlerFunc, middlewareFunc ...MiddlewareFunc) {
	r.handle(name, http.MethodHead, handlerFunc, middlewareFunc...)
}

//user  get->handle
//goods
//order
type router struct {
	routerGroups []*routerGroup
	engine       *Engine
}

func (r *router) Group(name string) *routerGroup {
	routerGroup := &routerGroup{
		name:               name,
		handleFuncMap:      make(map[string]map[string]HandlerFunc),
		middlewaresFuncMap: make(map[string]map[string][]MiddlewareFunc),
		handlerMethodMap:   make(map[string][]string),
		treeNode:           &treeNode{name: "/", children: make([]*treeNode, 0)},
	}
	routerGroup.Use(r.engine.middles...)
	r.routerGroups = append(r.routerGroups, routerGroup)
	return routerGroup
}

type ErrorHandler func(err error) (int, any)

type Engine struct {
	router
	funcMap          template.FuncMap
	HTMLRender       render.HTMLRender
	pool             sync.Pool
	Logger           *msLog.Logger
	middles          []MiddlewareFunc
	errorHandler     ErrorHandler
	OpenGateway      bool
	gatewayConfigs   []gateway.GWConfig
	gatewayTreeNode  *gateway.TreeNode
	gatewayConfigMap map[string]gateway.GWConfig
	RegisterType     string
	RegisterOption   register.Option
	RegisterCli      register.MsRegister
}

func New() *Engine {
	engine := &Engine{
		router:           router{},
		gatewayTreeNode:  &gateway.TreeNode{Name: "/", Children: make([]*gateway.TreeNode, 0)},
		gatewayConfigMap: make(map[string]gateway.GWConfig),
	}
	engine.pool.New = func() any {
		return engine.allocateContext()
	}
	return engine
}

func Default() *Engine {
	engine := New()
	engine.Logger = msLog.Default()
	logPath, ok := config.Conf.Log["path"]
	if ok {
		engine.Logger.SetLogPath(logPath.(string))
	}
	engine.Use(Logging, Recovery)
	engine.router.engine = engine
	return engine
}

func (e *Engine) allocateContext() any {
	return &Context{engine: e}
}

func (e *Engine) SetGatewayConfig(configs []gateway.GWConfig) {
	e.gatewayConfigs = configs
	//把这个路径 存储起来 访问的时候 去匹配这里面的路由 如果匹配，就拿出来相应的匹配结果
	for _, v := range e.gatewayConfigs {
		e.gatewayTreeNode.Put(v.Path, v.Name)
		e.gatewayConfigMap[v.Name] = v
	}
}

func (e *Engine) SetFuncMap(funcMap template.FuncMap) {
	e.funcMap = funcMap
}

func (e *Engine) LoadTemplate(pattern string) {
	t := template.Must(template.New("").Funcs(e.funcMap).ParseGlob(pattern))
	e.SetHtmlTemplate(t)
}

func (e *Engine) LoadTemplateConf() {
	pattern, ok := config.Conf.Template["pattern"]
	if ok {
		t := template.Must(template.New("").Funcs(e.funcMap).ParseGlob(pattern.(string)))
		e.SetHtmlTemplate(t)
	}
}

func (e *Engine) SetHtmlTemplate(t *template.Template) {
	e.HTMLRender = render.HTMLRender{Template: t}
}

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := e.pool.Get().(*Context)
	ctx.W = w
	ctx.R = r
	ctx.Logger = e.Logger
	e.httpRequestHandle(ctx, w, r)

	e.pool.Put(ctx)
}

func (e *Engine) httpRequestHandle(ctx *Context, w http.ResponseWriter, r *http.Request) {
	if e.OpenGateway {
		//请求过来，具体转发到哪？
		path := r.URL.Path
		node := e.gatewayTreeNode.Get(path)
		if node == nil {
			ctx.W.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(ctx.W, ctx.R.RequestURI+" not found")
			return
		}
		gwConfig := e.gatewayConfigMap[node.GwName]
		gwConfig.Header(ctx.R)
		addr, err := e.RegisterCli.GetValue(gwConfig.ServiceName)
		if err != nil {
			ctx.W.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(ctx.W, err.Error())
			return
		}
		target, err := url.Parse(fmt.Sprintf("http://%s%s", addr, path))
		if err != nil {
			ctx.W.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(ctx.W, err.Error())
			return
		}
		//网关的处理逻辑
		director := func(req *http.Request) {
			req.Host = target.Host
			req.URL.Host = target.Host
			req.URL.Path = target.Path
			req.URL.Scheme = target.Scheme
			if _, ok := req.Header["User-Agent"]; !ok {
				req.Header.Set("User-Agent", "")
			}
		}
		response := func(response *http.Response) error {
			log.Println("响应修改")
			return nil
		}
		handler := func(writer http.ResponseWriter, request *http.Request, err error) {
			log.Println(err)
			log.Println("错误处理")
		}
		proxy := httputil.ReverseProxy{Director: director, ModifyResponse: response, ErrorHandler: handler}
		proxy.ServeHTTP(w, r)
		return
	}
	method := r.Method
	for _, group := range e.routerGroups {
		routerName := SubStringLast(r.URL.Path, "/"+group.name)
		// get/1
		node := group.treeNode.Get(routerName)
		if node != nil && node.isEnd {
			//路由匹配上了
			handle, ok := group.handleFuncMap[node.routerName][ANY]
			if ok {
				group.methodHandle(node.routerName, ANY, handle, ctx)
				return
			}
			handle, ok = group.handleFuncMap[node.routerName][method]
			if ok {
				group.methodHandle(node.routerName, method, handle, ctx)
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Fprintf(w, "%s %s not allowed \n", r.RequestURI, method)
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
	fmt.Fprintf(w, "%s  not found \n", r.RequestURI)
}

func (e *Engine) Run(addr string) {
	//user  key:get value: func
	//for _, group := range e.routerGroups {
	//	for key, value := range group.handleFuncMap {
	//		http.HandlerFunc("/"+group.Name+key, value)
	//	}
	//}
	if e.RegisterType == "nacos" {
		r := &register.MsNacosRegister{}
		err := r.CreateCli(e.RegisterOption)
		if err != nil {
			panic(err)
		}
		e.RegisterCli = r
	}
	if e.RegisterType == "etcd" {
		r := &register.MsEtcdRegister{}
		err := r.CreateCli(e.RegisterOption)
		if err != nil {
			panic(err)
		}
		e.RegisterCli = r
	}
	http.Handle("/", e)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (e *Engine) RunTLS(addr, certFile, keyFile string) {
	err := http.ListenAndServeTLS(addr, certFile, keyFile, e.Handler())
	if err != nil {
		log.Fatal(err)
	}
}

func (e *Engine) Use(middles ...MiddlewareFunc) {
	e.middles = append(e.middles, middles...)
}

func (e *Engine) RegisterErrorHandler(handler ErrorHandler) {
	e.errorHandler = handler
}

func (e *Engine) Handler() http.Handler {
	return e
}
