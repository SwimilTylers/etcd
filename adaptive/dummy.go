package adaptive

import (
	"go.etcd.io/etcd/raft"
	"go.uber.org/zap"
	"sync"
)

type DummyMonitor struct {
	logger *zap.Logger

	mu sync.Mutex

	leader uint64
	self   uint64
	state  raft.StateType

	peer []uint64

	mustCritical bool
}

func (d *DummyMonitor) GetConfig() *PerceptibleConfig {
	d.mu.Lock()
	defer d.mu.Unlock()

	return &PerceptibleConfig{
		State:    d.state,
		Leader:   d.leader,
		Self:     d.self,
		Critical: d.mustCritical,
		Peers:    d.peer,
	}
}

func (d *DummyMonitor) SetConfig(config *PerceptibleConfig) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.state = config.State
	d.leader = config.Leader
	d.self = config.Self
	d.peer = config.Peers
	d.mustCritical = config.Critical

	return nil
}

func (d *DummyMonitor) Perceive(id uint64, isConnected bool) {}

func (d *DummyMonitor) IsCritical() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.mustCritical
}

func NewDummyMonitor(cfg *PerceptibleConfig) (*DummyMonitor, error) {
	ret := &DummyMonitor{}
	if err := ret.SetConfig(cfg); err != nil {
		return nil, err
	} else {
		return ret, nil
	}
}
