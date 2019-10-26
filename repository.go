package aggregates

type Repository interface {
	Load(id string, fn func(ok bool, agg Aggregate, err error))
	LoadWithVersion(id string, expectedVersion int64,fn func(ok bool, agg Aggregate, err error))
	Add(a Aggregate, fn func(ok bool, err error))
}
