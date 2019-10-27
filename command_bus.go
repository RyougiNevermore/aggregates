package aggregates

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

func NewSimpleCommandBus(buffer int) CommandBus {
	return &simpleCommandBus{
		running:  false,
		reqChan:  make(chan *commandRequest, buffer),
		count:    new(sync.WaitGroup),
		lock:     new(sync.Mutex),
		handlers: new(sync.Map),
		pool: sync.Pool{
			New: func() interface{} {
				return &commandRequest{}
			},
		},
	}
}

type simpleCommandBus struct {
	running  bool
	reqChan  chan *commandRequest
	count    *sync.WaitGroup
	lock     *sync.Mutex
	handlers *sync.Map
	pool     sync.Pool
}

func (bus *simpleCommandBus) getReq() (req *commandRequest) {
	req, _ = bus.pool.Get().(*commandRequest)
	return
}

func (bus *simpleCommandBus) putReq(req *commandRequest) {
	req.fn = nil
	req.msg = nil
	req.ctx = nil
	bus.pool.Put(req)
}

func (bus *simpleCommandBus) Subscribe(name string, handler CommandHandler) {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if !bus.running {
		panic(errors.New("command bus is not running"))
		return
	}
	bus.handlers.Store(name, handler)
	return
}

func (bus *simpleCommandBus) Unsubscribe(name string) {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if bus.running {
		panic(errors.New("command bus is running"))
		return
	}
	bus.handlers.Delete(name)
}

func (bus *simpleCommandBus) send(ctx context.Context, msg CommandMessage, fn func(ctx context.Context, id string, err error)) {
	bus.lock.Lock()
	if !bus.running {
		bus.lock.Unlock()
		panic(errors.New("command bus is not running"))
		return
	}
	bus.lock.Unlock()
	req := bus.getReq()
	req.msg = msg
	req.fn = fn
	req.ctx = ctx
	bus.reqChan <- req
}

func (bus *simpleCommandBus) dispatch(ctx context.Context, msg CommandMessage, fn func(ctx context.Context, id string, err error)) {
	handler0, has := bus.handlers.Load(msg.CommandName())
	if !has {
		fn(ctx,"", fmt.Errorf("can not found command handler via command name %s", msg.CommandName()))
		return
	}
	handler, isHandler := handler0.(CommandHandler)
	if !isHandler {
		panic(fmt.Sprintf("the expected command handler is not typed aggregates.CommandHandler, command name is %s", msg.CommandName()))
		return
	}
	handler.Handle(ctx, msg, fn)
	return
}

func (bus *simpleCommandBus) Start(ctx context.Context) (err error) {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if bus.running {
		return errors.New("command bus is running")
	}
	wg := new(sync.WaitGroup)
	go func(ctx context.Context, bus *simpleCommandBus, wg *sync.WaitGroup) {
		for {
			req, ok := <- bus.reqChan
			if !ok {
				break
			}
			bus.dispatch(req.ctx, req.msg, req.fn)
			bus.putReq(req)
		}
	}(ctx, bus, wg)
	wg.Wait()
	bus.running = true
	return
}

func (bus *simpleCommandBus) Shutdown(ctx context.Context, fn func(err error)) {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if !bus.running {
		fn(errors.New("command bus is not running"))
		return
	}
	close(bus.reqChan)
	go func(ctx context.Context, bus *simpleCommandBus, fn func(err error)) {
		bus.count.Wait()
		bus.running = false
		fn(nil)
	}(ctx, bus, fn)
}

func (bus *simpleCommandBus) ShutdownAndWait(ctx context.Context) (err error) {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if !bus.running {
		return errors.New("command bus is not running")
	}
	close(bus.reqChan)
	bus.count.Wait()
	bus.running = false
	return
}

type commandRequest struct {
	ctx context.Context
	msg CommandMessage
	fn  func(ctx context.Context, id string, err error)
}
