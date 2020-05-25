package aggregates

import (
	"container/list"
	"context"
	"errors"
)

func newContext(ctx context.Context, id string) *Context {
	return &Context{
		ctx:       ctx,
		id:        id,
		meta:      make(map[string]interface{}),
		events:    list.New(),
		committed: false,
	}
}

type Context struct {
	ctx       context.Context
	id        string
	meta      map[string]interface{}
	events    *list.List
	committed bool
}

func (ctx *Context) Context() context.Context {
	return ctx.ctx
}

func (ctx *Context) Id() (id string) {
	id = ctx.id
	return
}

func (ctx *Context) Put(key string, val interface{}) {
	ctx.meta[key] = val
}

func (ctx *Context) Get(key string) (val interface{}, has bool) {
	val, has = ctx.meta[key]
	return
}

func (ctx *Context) Apply(e DomainEvent) (err error) {
	if ctx.committed {
		err = errors.New("aggregates apply domain event failed, the context is committed")
		return
	}
	if e == nil {
		err = errors.New("aggregates apply domain event failed, the domain event is nil")
		return
	}
	ctx.events.PushBack(e)
	return
}

// 删除上一个domain event
func (ctx *Context) RejectLatest() {
	latest := ctx.events.Back()
	if latest == nil {
		return
	}
	ctx.events.Remove(latest)
}
