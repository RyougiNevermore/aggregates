package aggregates

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/vmihailenco/msgpack/v4"
	"time"
)

/* table ddl

-- Table: "DOC"."AGGREGATES"

-- DROP TABLE "DOC"."AGGREGATES";

CREATE TABLE "DOC"."AGGREGATES"
(
    "ID" character varying(64) COLLATE pg_catalog."default" NOT NULL,
    "NAME" character varying(256) COLLATE pg_catalog."default" NOT NULL,
    "EVENTS" jsonb NOT NULL DEFAULT '[]'::jsonb,
    "SNAPSHOTS" jsonb NOT NULL DEFAULT '[]'::jsonb,
    CONSTRAINT "AGGREGATES_pkey" PRIMARY KEY ("ID")
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE "DOC"."AGGREGATES"
    OWNER to yingli_root;

GRANT ALL ON TABLE "DOC"."AGGREGATES" TO yingli_root WITH GRANT OPTION;

-- Index: AGGREGATES_IDX_EVENTS

-- DROP INDEX "DOC"."AGGREGATES_IDX_EVENTS";

CREATE INDEX "AGGREGATES_IDX_EVENTS"
    ON "DOC"."AGGREGATES" USING gin
    ("EVENTS" jsonb_path_ops)
    TABLESPACE pg_default;

-- Index: AGGREGATES_IDX_NAME

-- DROP INDEX "DOC"."AGGREGATES_IDX_NAME";

CREATE INDEX "AGGREGATES_IDX_NAME"
    ON "DOC"."AGGREGATES" USING hash
    ("NAME" COLLATE pg_catalog."default")
    TABLESPACE pg_default;

-- Index: AGGREGATES_IDX_SNAPSHOTS

-- DROP INDEX "DOC"."AGGREGATES_IDX_SNAPSHOTS";

CREATE INDEX "AGGREGATES_IDX_SNAPSHOTS"
    ON "DOC"."AGGREGATES" USING gin
    ("SNAPSHOTS" jsonb_path_ops)
    TABLESPACE pg_default;

*/

type pgStoredEvent struct {
	Id    string    `json:"id"`
	Name  string    `json:"name"`
	Occur time.Time `json:"occur"`
	Data  []byte    `json:"data"`
}

func newMsgPackStoredEventFromPgStoredEvent(aggregateId string, pe *pgStoredEvent) *msgPackStoredEvent {
	return &msgPackStoredEvent{
		aggregateId:   aggregateId,
		aggregateName: "",
		eventName:     pe.Name,
		occur:         pe.Occur,
		data:          pe.Data,
	}
}

type pgStoredSnapshot struct {
	Data        []byte `json:"data"`
	LastEventId string `json:"lastEventId"`
}

const (
	sqlAggregateCheck          = `SELECT COUNT("ID") FROM "DOC"."AGGREGATES" WHERE "ID" = $1`
	sqlAggregateCreate         = `INSERT INTO "DOC"."AGGREGATES" ("ID", "NAME", "EVENTS", "SNAPSHOTS") VALUES ($1, $2, '[]', '[]')`
	sqlAggregateEventAppend    = `UPDATE "DOC"."AGGREGATES" SET  "EVENTS" = jsonb_insert("EVENTS", '{0}', $1) WHERE "ID" = $2 `
	sqlAggregateSnapshotAppend = `UPDATE "DOC"."AGGREGATES" SET  "SNAPSHOTS" = jsonb_insert("SNAPSHOTS", '{0}', $1) WHERE "ID" = $2 `
	sqlAggregateSnapshotGet    = `SELECT "SNAPSHOTS"->>0 AS "SNAPSHOT" FROM "DOC"."AGGREGATES" WHERE "ID" = $1`
	sqlAggregateEventList      = `SELECT jsonb_array_elements("EVENTS") FROM "DOC"."AGGREGATES" WHERE "ID" = $1 OFFSET $2 LIMIT $3`
	sqlAggregateEventListAll   = `SELECT jsonb_array_elements("EVENTS") FROM "DOC"."AGGREGATES" WHERE "ID" = $1 `
)

func NewPostgresEventStore(datasourceName string, maxOpenConns int, maxIdleConns int) EventStore {
	db, dbOpenErr := sql.Open("postgres", datasourceName)
	if dbOpenErr != nil {
		panic(fmt.Errorf("new postgres event store failed, %w", dbOpenErr))
		return nil
	}
	if pingErr := db.PingContext(context.TODO()); pingErr != nil {
		panic(fmt.Errorf("new postgres event store failed, %w", pingErr))
		return nil
	}
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	return &pgEventStore{db: db}
}

type pgEventStore struct {
	db *sql.DB
}

func (es *pgEventStore) Create(ctx context.Context, aggregateId string, aggregateName string) (err error) {
	qrs, queryErr := es.db.QueryContext(ctx, sqlAggregateCheck, aggregateId)
	if queryErr != nil {
		err = fmt.Errorf("event store create failed, %w", queryErr)
		return
	}
	has := false
	if qrs.Next() {
		count := int64(0)
		scanErr := qrs.Scan(&count)
		if scanErr != nil {
			err = fmt.Errorf("event store create failed, %w", scanErr)
			_ = qrs.Close()
			return
		}
		has = count > 0
	}
	_ = qrs.Close()
	if has {
		return
	}
	rs, insertErr := es.db.ExecContext(ctx, sqlAggregateCreate, &aggregateId, &aggregateName)
	if insertErr != nil {
		err = fmt.Errorf("event store create failed, %w", insertErr)
		return
	}
	num, _ := rs.RowsAffected()
	if num == 0 {
		err = fmt.Errorf("event store create failed, nothing is affected")
	}
	return
}

func (es *pgEventStore) AppendEvents(ctx context.Context, events []StoredEvent) (lastEventId string, err error) {

	tx, txErr := es.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
	if txErr != nil {
		err = fmt.Errorf("event store append event failed, %w", txErr)
		return
	}
	stmt, stmtErr := tx.PrepareContext(ctx, sqlAggregateEventAppend)
	if stmtErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("event store append event failed, %w", stmtErr)
		return
	}
	affected := int64(0)
	for _, event := range events {
		e := &pgStoredEvent{Id: aid.Next(), Name: event.EventName(), Occur: event.Occur(), Data: event.RawBytes()}
		eventRaw, eventRawErr := json.Marshal(e)
		if eventRawErr != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			err = fmt.Errorf("event store append event failed, %w", eventRawErr)
			return
		}
		rs, execErr := stmt.ExecContext(ctx, eventRaw, event.AggregateId())
		if execErr != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			err = fmt.Errorf("event store append event failed, %w", execErr)
			return
		}
		affected0, _ := rs.RowsAffected()
		affected = affected + affected0
		lastEventId = e.Id
	}
	if affected != int64(len(events)) {
		_ = stmt.Close()
		_ = tx.Rollback()
		err = fmt.Errorf("event store append event failed, not all events are saved")
		return
	}
	stmtCloseErr := stmt.Close()
	if stmtCloseErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("event store append event failed, %w", stmtCloseErr)
		return
	}
	commitErr := tx.Commit()
	if commitErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("event store append event failed, %w", commitErr)
		return
	}
	return
}

func (es *pgEventStore) ReadEvents(ctx context.Context, aggregateId string, lastEventId string) (events []StoredEvent, err error) {
	storedEvents := make([]*pgStoredEvent, 0, 1)
	if lastEventId == "" {
		// load all
		eventRs, eventListErr := es.db.QueryContext(ctx, sqlAggregateEventListAll, aggregateId)
		if eventListErr != nil {
			err = fmt.Errorf("event store read event failed, %w", eventListErr)
			return
		}
		for eventRs.Next() {
			var p []byte
			scanErr := eventRs.Scan(&p)
			if scanErr != nil {
				_ = eventRs.Close()
				err = fmt.Errorf("event store read event failed, %w", scanErr)
				return
			}
			if p == nil || len(p) == 0 {
				err = fmt.Errorf("event store read event failed, %w, aggregateId is %s", ErrNoStoredEventOfAggregate, aggregateId)
				return
			}
			event := &pgStoredEvent{}
			unmarshalErr := json.Unmarshal(p, event)
			if unmarshalErr != nil {
				_ = eventRs.Close()
				err = fmt.Errorf("event store read event failed, %w", unmarshalErr)
				return
			}
			storedEvents = append(storedEvents, event)
		}
		_ = eventRs.Close()

	} else {
		loops := 0
		maxLoops := 5
		offset := 0
		limit := 10
		got := false

		// 5 loops , 50 events
		for ; loops < maxLoops; loops++ {
			offset = limit * loops
			eventRs, eventListErr := es.db.QueryContext(ctx, sqlAggregateEventList, aggregateId, offset, limit)
			if eventListErr != nil {
				err = fmt.Errorf("event store read event failed, %w", eventListErr)
				return
			}
			hasDATA := false
			for eventRs.Next() {
				hasDATA = true
				var p []byte
				scanErr := eventRs.Scan(&p)
				if scanErr != nil {
					_ = eventRs.Close()
					err = fmt.Errorf("event store read event failed, %w", scanErr)
					return
				}
				if p == nil || len(p) == 0 {
					err = fmt.Errorf("event store read event failed, %w, aggregateId is %s", ErrNoStoredEventOfAggregate, aggregateId)
					return
				}
				event := &pgStoredEvent{}
				unmarshalErr := json.Unmarshal(p, event)
				if unmarshalErr != nil {
					_ = eventRs.Close()
					err = fmt.Errorf("event store read event failed, %w", unmarshalErr)
					return
				}
				if event.Id == lastEventId {
					got = true
					break
				}
				storedEvents = append(storedEvents, event)
			}
			_ = eventRs.Close()
			if !hasDATA {
				// no data
				break
			}
			if got {
				break
			}
		}
		if !got && loops == maxLoops {
			panic(fmt.Errorf("can not read %s aggregate domain events, cause the key event is lost, it can not be found in last 50 events", aggregateId))
		}
	}

	if len(storedEvents) == 0 {
		return
	}

	events = make([]StoredEvent, 0, 1)

	for _, e := range storedEvents {
		events = append(events, newMsgPackStoredEventFromPgStoredEvent(aggregateId, e))
	}

	return
}

func (es *pgEventStore) MakeSnapshot(ctx context.Context, aggregate Aggregate, lastEventId string) (err error) {

	tx, txErr := es.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
	if txErr != nil {
		err = fmt.Errorf("event store append snapshot failed, %w", txErr)
		return
	}
	stmt, stmtErr := tx.PrepareContext(ctx, sqlAggregateSnapshotAppend)
	if stmtErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("event store append snapshot failed, %w", stmtErr)
		return
	}

	sh := &pgStoredSnapshot{}
	sh.LastEventId = lastEventId
	sh.Data, err = msgpack.Marshal(aggregate)
	if err != nil {
		err = fmt.Errorf("event store append snapshot failed, %w", err)
		return
	}

	shRaw, shRawErr := json.Marshal(sh)
	if shRawErr != nil {
		_ = stmt.Close()
		_ = tx.Rollback()
		err = fmt.Errorf("event store append snapshot failed, %w", shRawErr)
		return
	}

	_, execErr := stmt.ExecContext(ctx, shRaw, aggregate.Identifier())
	if execErr != nil {
		_ = stmt.Close()
		_ = tx.Rollback()
		err = fmt.Errorf("event store append snapshot failed, %w", execErr)
		return
	}

	stmtCloseErr := stmt.Close()
	if stmtCloseErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("event store append snapshot failed, %w", stmtCloseErr)
		return
	}
	commitErr := tx.Commit()
	if commitErr != nil {
		_ = tx.Rollback()
		err = fmt.Errorf("event store append snapshot failed, %w", commitErr)
		return
	}

	////

	fmt.Println("snapshot,,,", aggregate)
	fmt.Println(sqlAggregateSnapshotAppend, string(shRaw), aggregate.Identifier())

	//
	return
}

func (es *pgEventStore) LoadSnapshot(ctx context.Context, aggregateId string, aggregate Aggregate) (lastEventId string, err error) {

	row, getSnapshotErr := es.db.QueryContext(ctx, sqlAggregateSnapshotGet, aggregateId)
	if getSnapshotErr != nil {
		err = fmt.Errorf("event store read snapshot failed, %w", getSnapshotErr)
		return
	}
	var snapshotRaw []byte
	if row.Next() {
		scanErr := row.Scan(&snapshotRaw)
		if scanErr != nil {
			_ = row.Close()
			err = fmt.Errorf("event store read snapshot failed, %w", scanErr)
			return
		}
	}
	_ = row.Close()
	if snapshotRaw == nil || len(snapshotRaw) == 0 {
		err = fmt.Errorf("event store read snapshot failed, %w, aggregateId is %s", ErrNoStoredSnapshotOfAggregate, aggregateId)
		return
	}
	snapshot := &pgStoredSnapshot{}
	unmarshalErr := json.Unmarshal(snapshotRaw, snapshot)
	if unmarshalErr != nil {
		err = fmt.Errorf("event store read snapshot failed, %w", unmarshalErr)
		return
	}

	err = msgpack.Unmarshal(snapshot.Data, aggregate)

	if err != nil {
		err = fmt.Errorf("event store read snapshot failed, %w", err)
		return
	}
	lastEventId = snapshot.LastEventId
	return
}
