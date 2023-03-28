package tracer

import (
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"io"
	"net/http"
)

func CreateTracer(serviceName string, samplerConfig *config.SamplerConfig, reporter *config.ReporterConfig, options ...config.Option) (opentracing.Tracer, io.Closer, error) {
	var cfg = config.Configuration{
		ServiceName: serviceName,
		Sampler:     samplerConfig,
		Reporter:    reporter,
	}
	tracer, closer, err := cfg.NewTracer(options...)
	return tracer, closer, err
}

func CreateTracerHeader(serviceName string, header http.Header, samplerConfig *config.SamplerConfig, reporter *config.ReporterConfig, options ...config.Option) (opentracing.Tracer, io.Closer, opentracing.SpanContext, error) {
	var cfg = config.Configuration{
		ServiceName: serviceName,
		Sampler:     samplerConfig,
		Reporter:    reporter,
	}
	tracer, closer, err := cfg.NewTracer(options...)
	// 继承别的进程传递过来的上下文
	spanContext, _ := tracer.Extract(opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(header))

	return tracer, closer, spanContext, err
}
