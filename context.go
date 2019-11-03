package aggregates

import (
	"context"
)

func NewContext(ctx context.Context) *Context {
	return &Context{
		context.WithValue(ctx, "id", newContextId()),
	}
}

func newContextId() string {
	return aid.Next()
}

type Context struct {
	context.Context
}

func (ctx *Context) Id() string {
	id, _ := ctx.Value("id").(string)
	return id
}
