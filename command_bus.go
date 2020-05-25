package aggregates

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	CommandBusNoHandleError    = errors.New("aggregates send command bus failed, handle is not found")
	CommandBusInterruptedError = errors.New("aggregates send command bus failed, command bus is interrupted")
)

type Command interface {
	TargetAggregateIdentifier() (id string)
}

type CommandHandle func(ctx *Context, command Command) (v interface{}, err error)

type CommandResult struct {
	id          string
	v           interface{}
	err         error
	interrupted bool
}

func (r *CommandResult) Id() string {
	return r.id
}

func (r *CommandResult) Value() interface{} {
	return r.v
}

func (r *CommandResult) Error() error {
	return r.err
}

func (r *CommandResult) Succeed() bool {
	return r.err == nil && !r.interrupted
}

func (r *CommandResult) Failed() bool {
	return r.err != nil || r.interrupted
}

// 中断：bus停止了，不再消费command了
func (r *CommandResult) Interrupted() bool {
	return r.interrupted
}

type CommandBus interface {
	Subscribe(name string, handle CommandHandle)
	Unsubscribe(name string)
	Send(name string, command Command) (id string, err error)
	SendAndWait(name string, command Command) (result *CommandResult)
	Start(ctx context.Context)
	ShutdownAndWait(ctx context.Context)
}

type commandMessage struct {
	id      string
	name    string
	command Command
	async   bool
	result  chan *CommandResult
}

var commandMessagePool = sync.Pool{New: func() interface{} {
	return &commandMessage{}
}}

func commandMessageAcquire(id string, name string, command Command, async bool, result chan *CommandResult) *commandMessage {
	msg := commandMessagePool.Get().(*commandMessage)
	msg.id = id
	msg.name = name
	msg.command = command
	msg.async = async
	if async {
		msg.result = result
	}
	return msg
}

func commandMessageRelease(msg *commandMessage) {
	if msg.async {
		close(msg.result)
	}
	commandMessagePool.Put(msg)
}

func NewSimpleCommandBus(workers int, buffer int) (bus CommandBus) {

	if workers < 1 {
		workers = runtime.NumCPU()
	}
	if buffer < 1 {
		buffer = 1024
	}

	bus = &simpleCommandBus{
		running: NewAtomicSwitch(),
		workers: workers,
		handles: &sync.Map{},
		wg:      &sync.WaitGroup{},
		ch:      make(chan *commandMessage, buffer),
	}

	return
}

type simpleCommandBus struct {
	running             *AtomicSwitch
	workers             int
	handles             *sync.Map
	wg                  *sync.WaitGroup
	ch                  chan *commandMessage
	cancelFn            context.CancelFunc
	resultsCache        *cacheTTL
	resultsCacheItemTTL time.Duration
}

func (s *simpleCommandBus) Subscribe(name string, handle CommandHandle) {
	if s.running.IsOn() {
		panic(errors.New("command bus subscribe handle failed, it is running"))
	}
	name = strings.TrimSpace(name)
	if name == "" {
		panic(errors.New("command bus subscribe handle failed, name is empty"))
	}
	if handle == nil {
		panic(errors.New("command bus subscribe handle failed, handle is nil"))
	}
	_, has := s.handles.Load(name)
	if has {
		panic(errors.New("command bus subscribe handle failed, handle existed"))
	}
	s.handles.Store(name, handle)
}

func (s *simpleCommandBus) Unsubscribe(name string) {
	if s.running.IsOn() {
		panic(errors.New("command bus unsubscribe handle failed, it is running"))
	}
	name = strings.TrimSpace(name)
	if name == "" {
		panic(errors.New("command bus unsubscribe handle failed, name is empty"))
	}
	s.handles.Delete(name)
}

func (s *simpleCommandBus) getHandle(name string) (handle CommandHandle, has bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		panic(errors.New("command bus get handle failed, name is empty"))
	}
	var h0 interface{}
	h0, has = s.handles.Load(name)
	if !has {
		return
	}

	handle, has = h0.(CommandHandle)

	return
}

func (s *simpleCommandBus) Send(name string, command Command) (id string, err error) {
	name = strings.TrimSpace(name)
	if name == "" {
		panic(errors.New("command bus send command failed, name is empty"))
	}
	if command == nil {
		panic(errors.New("command bus send command failed, command is nil"))
	}
	id = uuid.New().String()
	msg := commandMessageAcquire(id, name, command, false, nil)
	if s.running.IsOff() {
		commandMessageRelease(msg)
		err = errors.New("command bus send command failed, it is not running")
		return
	}
	s.ch <- msg
	return
}

func (s *simpleCommandBus) SendAndWait(name string, command Command) (result *CommandResult) {
	name = strings.TrimSpace(name)
	if name == "" {
		panic(errors.New("command bus send command failed, name is empty"))
	}
	if command == nil {
		panic(errors.New("command bus send command failed, command is nil"))
	}
	id := uuid.New().String()
	resultCh := make(chan *CommandResult, 1)
	msg := commandMessageAcquire(id, name, command, true, resultCh)
	if s.running.IsOff() {
		commandMessageRelease(msg)
		result = &CommandResult{
			id:  id,
			v:   nil,
			err: errors.New("command bus send command failed, it is not running"),
		}
		return
	}

	s.ch <- msg

	result, _ = <-resultCh
	if result == nil {
		result = &CommandResult{
			id:  id,
			v:   nil,
			err: errors.New("command bus send command failed, recv empty result"),
		}
	}

	return
}

func (s *simpleCommandBus) listen(ctx context.Context) {
	stopping := false
	stop := false
	for {
		select {
		case <-ctx.Done():
			stopping = true
		case msg, ok := <-s.ch:
			if !ok {
				stop = true
				break
			}
			var result *CommandResult
			if stopping {
				result = &CommandResult{
					id:          msg.id,
					v:           nil,
					err:         CommandBusInterruptedError,
					interrupted: true,
				}
				if msg.async {
					msg.result <- result
				}
				s.resultsCache.put(msg.id, result, s.resultsCacheItemTTL)
				commandMessageRelease(msg)
				continue
			}
			handle, has := s.getHandle(msg.name)
			if !has {
				result = &CommandResult{
					id:          msg.id,
					v:           nil,
					err:         CommandBusNoHandleError,
					interrupted: false,
				}
				if msg.async {
					msg.result <- result
				}
				s.resultsCache.put(msg.id, result, s.resultsCacheItemTTL)
				commandMessageRelease(msg)
				continue
			}
			resultV, handleErr := handle(newContext(ctx, msg.id), msg.command)
			result = &CommandResult{
				id:          msg.id,
				v:           resultV,
				err:         handleErr,
				interrupted: false,
			}
			if msg.async {
				msg.result <- result
			}
			s.resultsCache.put(msg.id, result, s.resultsCacheItemTTL)
			commandMessageRelease(msg)
		}
		if stop {
			break
		}
	}
}

func (s *simpleCommandBus) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.TODO()
	}
	ctx, s.cancelFn = context.WithCancel(ctx)
	s.resultsCache.loopClean(ctx)
	for i := 0; i < s.workers; i++ {
		go func(ctx context.Context, s *simpleCommandBus) {
			s.listen(ctx)
		}(ctx, s)
	}
}

func (s *simpleCommandBus) ShutdownAndWait(ctx context.Context) {
	panic("implement me")
}
