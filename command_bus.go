package aggregates

import (
	"context"
	"fmt"
	"sync"
)

type simpleCommandMessage struct {
	ctx        *Context
	name       string
	command    Command
	resultChan chan *simpleCommandResult
}

var simpleCommandMessagePool = &sync.Pool{
	New: func() interface{} {
		return &simpleCommandMessage{}
	},
}

func asSimpleCommandMessage(ctx *Context, name string, command Command, resultChan chan *simpleCommandResult) *simpleCommandMessage {
	msg0 := simpleCommandMessagePool.Get()
	msg, _ := msg0.(*simpleCommandMessage)
	msg.ctx = ctx
	msg.command = command
	msg.name = name
	msg.resultChan = resultChan
	return msg
}

func releaseSimpleCommandMessage(msg *simpleCommandMessage) {
	msg.ctx = nil
	msg.name = ""
	msg.command = nil
	msg.resultChan = nil
	simpleCommandMessagePool.Put(msg)
}

type simpleCommandResult struct {
	id  string
	err error
}

var simpleCommandResultPool = &sync.Pool{
	New: func() interface{} {
		return &simpleCommandResult{}
	},
}

func asSimpleCommandResult(id string, err error) *simpleCommandResult {
	result0 := simpleCommandResultPool.Get()
	result, _ := result0.(*simpleCommandResult)
	result.id = id
	result.err = err
	return result
}

func releaseSimpleCommandResult(result *simpleCommandResult) {
	result.id = ""
	result.err = nil
	simpleCommandResultPool.Put(result)
}

type unitOfWorker struct {
	count    int
	workerNo int64
}

func NewSimpleCommandBus(buffer int, workers int) CommandBus {
	return &simpleCommandBus{
		running:              false,
		runMutex:             new(sync.Mutex),
		runWg:                new(sync.WaitGroup),
		buffer:               buffer,
		workers:              int64(workers),
		commandMasterQueue:   nil,
		commandWorkerQueues:  make([]chan *simpleCommandMessage, workers),
		commandWorkerIndex:   0,
		commandWorkerMask:    int64(workers - 1),
		commandWorkerMapping: new(sync.Map),
		handleMap:            new(sync.Map),
	}
}

type simpleCommandBus struct {
	running             bool
	runMutex            *sync.Mutex
	runWg               *sync.WaitGroup
	buffer              int
	workers             int64
	commandMasterQueue  chan *simpleCommandMessage
	commandWorkerQueues []chan *simpleCommandMessage

	commandWorkerIndex   int64
	commandWorkerMask    int64
	commandWorkerMapping *sync.Map

	handleMap *sync.Map
}

func (bus *simpleCommandBus) sendToWorker(msg *simpleCommandMessage) {
	workerKey := msg.command.TargetAggregateIdentifier()
	var unit *unitOfWorker
	unit0, has := bus.commandWorkerMapping.Load(workerKey)
	workNo := int64(0)
	if !has {
		workNo = bus.commandWorkerIndex & bus.commandWorkerMask
		unit = &unitOfWorker{count: 0, workerNo: workNo}
		bus.commandWorkerMapping.Store(workerKey, unit)
		bus.commandWorkerIndex++
	} else {
		unit, _ = unit0.(*unitOfWorker)
		workNo = unit.workerNo
	}
	bus.commandWorkerQueues[workNo] <- msg
	unit.count++
}

func (bus *simpleCommandBus) releaseFromWorker(msg *simpleCommandMessage) {
	workerKey := msg.command.TargetAggregateIdentifier()
	if unit0, has := bus.commandWorkerMapping.Load(workerKey); has {
		unit, _ := unit0.(*unitOfWorker)
		unit.count--
		if unit.count == 0 {
			bus.commandWorkerMapping.Delete(workerKey)
		}
	}
}

func (bus *simpleCommandBus) getHandle(name string) (handle CommandHandle) {
	if handle0, has := bus.handleMap.Load(name); has {
		handle, _ = (handle0).(CommandHandle)
	}
	return
}

func (bus *simpleCommandBus) Subscribe(name string, handle CommandHandle) {
	bus.runMutex.Lock()
	defer bus.runMutex.Unlock()
	if bus.running {
		panic(fmt.Errorf("command bus can not subscribe the %s command handle, %w", name, CommandBusIsRunningErr))
	}
	if name == "" {
		panic(fmt.Errorf("command bus can not subscribe the empty name command handle"))
	}
	if handle == nil {
		panic(fmt.Errorf("command bus can not subscribe the %s command handle, cause handle is nil", name))
	}
	bus.handleMap.Store(name, handle)
}

func (bus *simpleCommandBus) Unsubscribe(name string) {
	bus.runMutex.Lock()
	defer bus.runMutex.Unlock()
	if bus.running {
		panic(fmt.Errorf("command bus can not unsubscribe the %s command handle, %w", name, CommandBusIsRunningErr))
	}
	if name == "" {
		panic(fmt.Errorf("command bus can not unsubscribe the empty name command handle"))
	}
	bus.handleMap.Delete(name)
}

func (bus *simpleCommandBus) Send(ctx *Context, name string, command Command) (id string, err error) {
	//bus.runMutex.Lock()
	//defer bus.runMutex.Unlock()
	if !bus.running {
		panic(fmt.Errorf("can not send command into the command bus, %w", CommandBusIsNotRunningErr))
	}
	if name == "" {
		err = SendNoNameCommandErr
		return
	}
	if command == nil {
		err = SendEmptyCommandErr
		return
	}
	resultChan := make(chan *simpleCommandResult, 1)
	bus.commandMasterQueue <- asSimpleCommandMessage(ctx, name, command, resultChan)
	bus.runWg.Add(1)
	result := <-resultChan
	id, err = result.id, result.err
	releaseSimpleCommandResult(result)
	result = nil
	return
}

func (bus *simpleCommandBus) dispatch(ctx *Context, name string, command Command) (id string, err error) {
	handle := bus.getHandle(name)
	if handle == nil {
		panic(fmt.Errorf("comand bus dispatch command failed, cause the %s handle is nil", name))
	}
	id, err = handle(ctx, command)
	return
}

func (bus *simpleCommandBus) Start(ctx context.Context) {
	bus.runMutex.Lock()
	defer bus.runMutex.Unlock()
	if bus.running {
		panic(fmt.Errorf("startup the command bus failed, %w", CommandBusIsRunningErr))
	}
	bus.running = true
	bus.createQueue()
	bus.runWg.Add(1)
	go func(bus *simpleCommandBus) {
		for i := int64(0); i < bus.workers; i++ {
			bus.runWg.Add(1)
			go func(workerNo int64, bus *simpleCommandBus) {
				bus.runWg.Done()
				worker := bus.commandWorkerQueues[workerNo]
				for {
					msg, ok := <-worker
					if !ok {
						break
					}
					id, err := bus.dispatch(msg.ctx, msg.name, msg.command)

					msg.resultChan <- asSimpleCommandResult(id, err)
					close(msg.resultChan)

					bus.releaseFromWorker(msg)
					releaseSimpleCommandMessage(msg)

					bus.runWg.Done()
				}
				bus.commandWorkerQueues[workerNo] = nil

			}(i, bus)
		}

		bus.runWg.Done()
		for {
			msg, ok := <-bus.commandMasterQueue
			if !ok {
				break
			}
			bus.sendToWorker(msg)
		}
		bus.commandMasterQueue = nil
		for _, worker := range bus.commandWorkerQueues {
			close(worker)
		}
	}(bus)
	bus.runWg.Wait()
	return
}

func (bus *simpleCommandBus) createQueue() {
	bus.commandMasterQueue = make(chan *simpleCommandMessage, bus.buffer)
	for i := int64(0); i < bus.workers; i++ {
		worker := make(chan *simpleCommandMessage, bus.buffer)
		bus.commandWorkerQueues[i] = worker
	}
}

func (bus *simpleCommandBus) ShutdownAndWait(ctx context.Context) {
	bus.runMutex.Lock()
	defer bus.runMutex.Unlock()
	if !bus.running {
		panic(fmt.Errorf("shutdown the command bus failed, %w", CommandBusIsNotRunningErr))
	}
	bus.running = false
	close(bus.commandMasterQueue)
	bus.runWg.Wait()
	return
}
