package aggregates

import (
	"context"
	"errors"
	"github.com/vmihailenco/msgpack/v4"
	"time"
)

var (
	ErrNoStoredEventOfAggregate    = errors.New("no stored event of aggregate")
	ErrNoStoredSnapshotOfAggregate = errors.New("no stored snapshot of aggregate")
)

type StoredEvent interface {
	DomainEventRaw
	AggregateName() string
	AggregateId() string
	Encode(v interface{}) error
	RawBytes() []byte
	Occur() time.Time
}

type EventStore interface {
	Create(ctx context.Context, aggregateId string, aggregateName string) (err error)
	AppendEvents(ctx context.Context, events []StoredEvent) (lastEventId string, err error)
	ReadEvents(ctx context.Context, aggregateId string) (events []StoredEvent, err error)
	MakeSnapshot(ctx context.Context, aggregate Aggregate, lastEventId string) (err error)
	LoadSnapshot(ctx context.Context, aggregateId string, aggregate Aggregate) (err error)
}

func asMsgPackStoredEvent(aggregateName string, eventName string, event DomainEvent, occur time.Time) *msgPackStoredEvent {
	se := &msgPackStoredEvent{}
	se.aggregateName = aggregateName
	se.aggregateId = event.AggregateIdentifier()
	se.eventName = eventName
	se.occur = occur
	_ = se.Encode(event)
	return se
}

type msgPackStoredEvent struct {
	aggregateId   string
	aggregateName string
	eventName     string
	occur         time.Time
	data          []byte
}

func (e *msgPackStoredEvent) AggregateName() string {
	return e.aggregateName
}

func (e *msgPackStoredEvent) AggregateId() string {
	return e.aggregateId
}

func (e *msgPackStoredEvent) RawBytes() []byte {
	return e.data
}

func (e *msgPackStoredEvent) Occur() time.Time {
	return e.occur
}

func (e *msgPackStoredEvent) EventName() string {
	return e.eventName
}

func (e *msgPackStoredEvent) Decode(event DomainEvent) (err error) {
	err = msgpack.Unmarshal(e.data, event)
	return
}

func (e *msgPackStoredEvent) Encode(event interface{}) (err error) {
	e.data, err = msgpack.Marshal(event)
	return
}
