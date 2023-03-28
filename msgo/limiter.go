package msgo

import (
	"context"
	"golang.org/x/time/rate"
	"net/http"
	"time"
)

func Limiter(limit, cap int) MiddlewareFunc {
	li := rate.NewLimiter(rate.Limit(limit), cap)
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx *Context) {
			//实现限流
			con, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
			defer cancel()
			err := li.WaitN(con, 1)
			if err != nil {
				ctx.String(http.StatusForbidden, "限流了")
				return
			}
			next(ctx)
		}
	}
}
