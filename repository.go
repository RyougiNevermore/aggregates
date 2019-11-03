package aggregates

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func newTxContext(ctx context.Context) *TxContext {
	return &TxContext{
		ctx,
		make([]*aggregateEvent, 0, 1),
	}
}

type TxContext struct {
	context.Context
	events []*aggregateEvent
}

func (ctx *TxContext) Apply(name string, event DomainEvent) {
	ctx.events = append(ctx.events, &aggregateEvent{name: name, payload: event, occur: time.Now()})
}

func (ctx *TxContext) unCommitEvents() []*aggregateEvent {
	return ctx.events
}

type AggregateSaveListener interface {
	Handle(aggregate Aggregate)
}

type Repository interface {
	Name() string
	BeginTx(ctx context.Context) *TxContext
	Load(ctx context.Context, id string, agg Aggregate) (has bool, err error)
	LoadWithVersion(ctx context.Context, id string, expectedVersion, agg Aggregate) (has bool, err error)
	Save(ctx *TxContext, a Aggregate) (ok bool, err error)
	SetAggregateSaveListener(listener AggregateSaveListener)
}

func NewSimpleRepository(aggregateName string, eventStore EventStore) Repository {
	return &simpleRepository{
		aggregateName: aggregateName,
		eventStore:    eventStore,
		eventBus:      nil,
	}
}

func NewSimpleRepositoryWithEventBus(aggregateName string, eventStore EventStore, eventBus EventBus) Repository {
	return &simpleRepository{
		aggregateName: aggregateName,
		eventStore:    eventStore,
		eventBus:      eventBus,
	}
}

type simpleRepository struct {
	aggregateName         string
	eventStore            EventStore
	eventBus              EventBus
	aggregateSaveListener AggregateSaveListener
}

func (r *simpleRepository) Name() string {
	return r.aggregateName
}

func (r *simpleRepository) BeginTx(ctx context.Context) *TxContext {
	return newTxContext(ctx)
}

func (r *simpleRepository) SetAggregateSaveListener(listener AggregateSaveListener) {
	r.aggregateSaveListener = listener
}

func (r *simpleRepository) Load(ctx context.Context, id string, agg Aggregate) (has bool, err error) {
	lastEventId, loadSnapshotErr := r.eventStore.LoadSnapshot(ctx, id, agg)
	if loadSnapshotErr != nil {
		if errors.Is(loadSnapshotErr, ErrNoStoredSnapshotOfAggregate) {
			has = false
			return
		}
		err = fmt.Errorf("load aggregate from repository failed, %w", loadSnapshotErr)
		return
	}
	has = true
	// load events
	events, eventsLoadErr := r.eventStore.ReadEvents(ctx, id, lastEventId)
	if eventsLoadErr != nil {
		err = fmt.Errorf("load aggregate from repository failed, %w", eventsLoadErr)
		return
	}
	if events != nil {
		for _, event := range events {
			agg.OnEvent(event.EventName(), event)
		}
	}
	return
}

func (r *simpleRepository) LoadWithVersion(ctx context.Context, id string, expectedVersion, agg Aggregate) (has bool, err error) {
	panic(fmt.Errorf("load aggregate from repository with version failed, cause it is not implemented"))
	return
}

func (r *simpleRepository) Save(ctx *TxContext, aggregate Aggregate) (ok bool, err error) {
	createErr := r.eventStore.Create(ctx, aggregate.Identifier(), r.aggregateName)
	if createErr != nil {
		err = fmt.Errorf("save aggregate into repository failed, %w", createErr)
		return
	}
	aggEvents := ctx.unCommitEvents()
	storedEvents := make([]StoredEvent, len(aggEvents))
	for i, aggEvent := range aggEvents {
		storedEvents[i] = asMsgPackStoredEvent(r.aggregateName, aggEvent.name, aggEvent.payload, aggEvent.occur)
	}

	lastEventId, appendEventErr := r.eventStore.AppendEvents(ctx, storedEvents)
	if appendEventErr != nil {
		err = fmt.Errorf("save aggregate into repository failed, %w", appendEventErr)
		return
	}

	makeSnapshotErr := r.eventStore.MakeSnapshot(ctx, aggregate, lastEventId)
	if makeSnapshotErr != nil {
		err = fmt.Errorf("save aggregate into repository failed, %w", makeSnapshotErr)
		return
	}
	ok = true

	eventBus := r.eventBus
	if eventBus != nil {
		for _, storedEvent := range storedEvents {
			eventBus.Publish(asSimpleEventMessage(storedEvent))
		}
	}
	if r.aggregateSaveListener != nil {
		r.aggregateSaveListener.Handle(aggregate)
	}
	return
}
