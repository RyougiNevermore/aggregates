package aggregates

import (
	"context"
	"errors"
)

var (
	CommandBusIsRunningErr    = errors.New("command bus is running")
	CommandBusIsNotRunningErr = errors.New("command bus is not running")
	SendNoNameCommandErr      = errors.New("send command failed, cause name is empty")
	SendEmptyCommandErr       = errors.New("send command failed, cause the command is nil")
)

type Command interface {
	TargetAggregateIdentifier() (id string)
}

type CommandHandle func(ctx *Context, command Command) (id string, err error)

type CommandBus interface {
	Subscribe(name string, handle CommandHandle)
	Unsubscribe(name string)
	Send(ctx *Context, name string, command Command) (id string, err error)
	dispatch(ctx *Context, name string, command Command) (id string, err error)
	Start(ctx context.Context)
	ShutdownAndWait(ctx context.Context)
}
