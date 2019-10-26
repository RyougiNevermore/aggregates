package aggregates

type Aggregate interface {
	Identifier() (id string)
}
