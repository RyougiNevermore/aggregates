package aggregates

import (
	"runtime"
	"sync/atomic"
	"time"
)

const (
	atomicSwitchOff = int64(0)
	atomicSwitchOn  = int64(1)
)

func NewAtomicSwitch() *AtomicSwitch {
	return &AtomicSwitch{
		status:  atomicSwitchOff,
		padding: [7]int64{0, 0, 0, 0, 0, 0, 0},
	}
}

type AtomicSwitch struct {
	status  int64
	padding [7]int64
}

func (s *AtomicSwitch) IsOn() bool {
	return atomic.LoadInt64(&s.status) == atomicSwitchOn
}

func (s *AtomicSwitch) IsOff() bool {
	return atomic.LoadInt64(&s.status) == atomicSwitchOff
}

func (s *AtomicSwitch) On() {

	if s.IsOn() {
		return
	}

	ok := false
	times := 10
	for {
		ok = atomic.CompareAndSwapInt64(&s.status, int64(0), int64(1))
		if ok {
			break
		}
		times--
		if times < 0 {
			runtime.Gosched()
			continue
		}
		time.Sleep(time.Nanosecond * 100)
	}

}

func (s *AtomicSwitch) Off() {

	if s.IsOff() {
		return
	}

	ok := false
	times := 10
	for {
		ok = atomic.CompareAndSwapInt64(&s.status, int64(1), int64(0))
		if ok {
			break
		}
		times--
		if times < 0 {
			runtime.Gosched()
			continue
		}
		time.Sleep(time.Nanosecond * 100)
	}

}

