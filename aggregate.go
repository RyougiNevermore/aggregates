package aggregates

import (
	"github.com/nats-io/nuid"
	"time"
)

type DomainEvent interface {
	AggregateIdentifier() string
}

type DomainEventRaw interface {
	EventName() string
	Decode(e DomainEvent) (err error)
}

type Aggregate interface {
	Identifier() (id string)
	OnEvent(name string, event DomainEventRaw)
}

var aid = nuid.New()

func NewAggregateId() (id string) {
	id = aid.Next()
	return
}

type aggregateEvent struct {
	name    string
	payload DomainEvent
	occur   time.Time
}
