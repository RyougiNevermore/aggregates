package aggregates

import "context"

type Command interface {
	TargetAggregateIdentifier() (id string)
	Name() (name string)
}

type CommandMessage interface {
	CommandName() (name string)
	Identifier() (id string)
	MetaData() *MetaData
	WithMetaData(metadata map[string]interface{})
	AndMetaData(metadata map[string]interface{})
	Payload() Command
}

type CommandHandler interface {
	Handle(ctx context.Context, msg CommandMessage) (id string, err error)
}

type CommandBus interface {
	Subscribe(name string, handler CommandHandler)
	Unsubscribe(name string)
	dispatch(msg CommandMessage, fn func(id string, err error)) (err error)
	Start(ctx context.Context) (err error)
	Shutdown(ctx context.Context, fn func(err error))
	ShutdownAndWait(ctx context.Context) (err error)
}

type CommandGateway interface {
	Send(ctx context.Context, cmd Command, fn func(id string, err error))
	SendAndWait(ctx context.Context, cmd Command) (id string, err error)
}

