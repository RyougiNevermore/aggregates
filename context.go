package aggregates

import "context"

func NewContext(ctx context.Context) *Context {

	return &Context{
		ctx,
		newContextId(),
	}
}

func newContextId() string {
	return aid.Next()
}

type Context struct {
	context.Context
	Id string
}
