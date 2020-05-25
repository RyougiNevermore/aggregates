package aggregates

var repositoryTxKey = "_repositoryTxKey"

type Repository interface {
	Name() string
	Load(ctx *Context, id string, a Aggregate) (has bool, err error)
	Begin(ctx *Context) (err error)
	Save(ctx *Context, a Aggregate) (err error)
	Commit(ctx *Context) (err error)
}

/* todo impl

	use event store to load, save and commit

	load = load snapshot + load events
	save = append events + make snapshot

 */