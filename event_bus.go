package aggregates

import "time"

type EventMessage interface {
	EventName() string
	AggregateName() string
	AggregateId() string
	Occur() time.Time
	Payload() []byte
}

type EventListener interface {
}

type EventBus interface {
	Publish(events ...EventMessage)
	Subscribe(name string, event EventMessage)
}

func asSimpleEventMessage(storedEvent StoredEvent) EventMessage {
	return &simpleEventMessage{
		aggregateName: storedEvent.AggregateName(),
		aggregateId:   storedEvent.AggregateId(),
		eventName:     storedEvent.EventName(),
		occur:         storedEvent.Occur(),
		payload:       storedEvent.RawBytes(),
	}
}

type simpleEventMessage struct {
	aggregateName string
	aggregateId   string
	eventName     string
	occur         time.Time
	payload       []byte
}

func (msg *simpleEventMessage) EventName() string {
	return msg.eventName
}

func (msg *simpleEventMessage) AggregateName() string {
	return msg.aggregateName
}

func (msg *simpleEventMessage) AggregateId() string {
	return msg.aggregateId
}

func (msg *simpleEventMessage) Occur() time.Time {
	return msg.occur
}

func (msg *simpleEventMessage) Payload() []byte {
	return msg.payload
}
