package draft

import (
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"sync"
)

type ParallelInterpreter interface {
	Start() error
	Stop() error

	SetResponseChannel(c chan<- *raftpb.Message) error
	InterpretParallel(m []*raftpb.Message) error
}

type parallelInterpreter struct {
	lg *zap.Logger

	cOut chan<- *raftpb.Message

	bufSize int

	mu *sync.Mutex

	itp       Interpreter
	reach     []uint64
	onRunning bool

	mBuf  map[uint64][]*raftpb.Message
	cIn   map[uint64]chan<- *raftpb.Message
	cStop map[uint64]chan<- *raftpb.Message
}

func (pi *parallelInterpreter) Start() error {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	if pi.onRunning {
		return ErrDupStart
	}

	pi.launch()

	return nil
}

func (pi *parallelInterpreter) Stop() error {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	if pi.onRunning {
		pi.shut()
	}

	return nil
}

func (pi *parallelInterpreter) SetResponseChannel(c chan<- *raftpb.Message) error {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	if pi.onRunning {
		return ErrUnchangeable
	}

	pi.cOut = c
	return nil
}

func (pi *parallelInterpreter) InterpretParallel(m []*raftpb.Message) error {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	for _, msg := range m {
		if in, ok := pi.cIn[msg.To]; ok {
			in <- msg
		} else {
			pi.lg.Warn("unknown destination, drop it", zap.Uint64("to", msg.To))
		}
	}

	return nil
}

func (pi *parallelInterpreter) shut() {
	for _, r := range pi.reach {
		close(pi.cStop[r])
		close(pi.cIn[r])
	}

	pi.mBuf = nil
	pi.cIn = nil
	pi.cStop = nil

	pi.onRunning = false
}

func (pi *parallelInterpreter) launch() {
	pi.mBuf = make(map[uint64][]*raftpb.Message, len(pi.reach))
	pi.cIn = make(map[uint64]chan<- *raftpb.Message, len(pi.reach))
	pi.cStop = make(map[uint64]chan<- *raftpb.Message, len(pi.reach))

	for _, r := range pi.reach {
		pi.start(r)
	}

	pi.onRunning = true
}

func (pi *parallelInterpreter) start(r uint64) {
	in := make(chan *raftpb.Message, pi.bufSize)
	stop := make(chan *raftpb.Message, pi.bufSize)

	go pi.run(r, in, stop, pi.cOut)

	pi.mBuf[r] = make([]*raftpb.Message, 0, pi.bufSize)
	pi.cIn[r] = in
	pi.cStop[r] = stop
}

func (pi *parallelInterpreter) run(serviceId uint64, cIn, cStop <-chan *raftpb.Message, cOut chan<- *raftpb.Message) {
Events:
	for {
		select {
		case m := <-cIn:
			if m == nil {
				pi.lg.Error("draft transporter service received a nil incoming message",
					zap.Uint64("service-id", serviceId),
				)
				continue
			}

			if pi.itp.IsSupported(m) {
				if resp := pi.itp.Interpret(m); m != nil {
					cOut <- resp
				}
			} else {
				pi.lg.Warn("draft transporter received an unsupported message, drop it",
					zap.Uint64("service-id", serviceId),
					zap.String("message-type", m.Type.String()),
				)
			}
		case <-cStop:
			break Events
		}
	}

	pi.lg.Info("draft transporter service stopped",
		zap.Uint64("service-id", serviceId),
	)
}
