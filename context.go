package aggregates

import "context"

func NewContext(ctx context.Context) *Context {

	return &Context{ctx}
}

type Context struct {
	context.Context
}
