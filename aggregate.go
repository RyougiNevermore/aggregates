package aggregates

import "github.com/nats-io/nuid"

type Aggregate interface {
	Identifier() (id string)
}

var aid = nuid.New()

func NewAggregateId() (id string) {
	id = aid.Next()
	return
}
