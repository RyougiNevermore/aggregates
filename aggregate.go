package aggregates

import (
	"time"
)

type DomainEvent interface {
	AggregateIdentifier() (id string)
	EventName() (name string)
}

type DomainEventRaw interface {
	EventName() string
	Decode(e DomainEvent) (err error)
}

type Aggregate interface {
	Identifier() (id string)
	OnEvent(name string, event DomainEvent)
}


type aggregateEvent struct {
	name    string
	payload DomainEvent
	occur   time.Time
}
