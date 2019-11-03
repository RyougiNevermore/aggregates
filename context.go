package aggregates

import (
	"context"
	"time"
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

func (ctx *Context) WithAggregate(aggregateName string) *AggregateContext {
	return &AggregateContext{
		context.WithValue(ctx, "aggName", aggregateName),
		make([]*aggregateEvent, 0, 1),
	}
}

type AggregateContext struct {
	context.Context
	events []*aggregateEvent
}

func (ctx *AggregateContext) Name() string {
	aggName, _ := ctx.Value("aggName").(string)
	return aggName
}

func (ctx *AggregateContext) Apply(name string, event DomainEvent) {
	ctx.events = append(ctx.events, &aggregateEvent{name: name, payload: event, occur: time.Now()})
}
