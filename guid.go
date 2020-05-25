package aggregates

import "github.com/google/uuid"

func NewAggregateId() (id string) {
	id = uuid.New().String()
	return
}
