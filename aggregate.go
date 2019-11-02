package aggregates

import (
	"github.com/nats-io/nuid"
	"time"
)

type DomainEvent interface {
	AggregateIdentifier() string
	Occur() time.Time
}

type DomainEventRaw interface {
	Decode(e DomainEvent) (err error)
}

type Aggregate interface {
	Identifier() (id string)
	IncrVersion() (version int64)
	onEvents(name string, event DomainEvent)
}

var aid = nuid.New()

func NewAggregateId() (id string) {
	id = aid.Next()
	return
}

