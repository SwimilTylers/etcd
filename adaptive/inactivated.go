package adaptive

import (
	"go.etcd.io/etcd/raft"
	"go.uber.org/zap"
	"sync"
)

type InactivatedMonitor struct {
	logger *zap.Logger

	mu sync.Mutex

	leader uint64
	self   uint64
	state  raft.StateType

	peer []uint64

	mustCritical bool

	activation func(logger *zap.Logger, config *PerceptibleConfig) (Perceptible, error)
}

func (iam *InactivatedMonitor) GetConfig() *PerceptibleConfig {
	iam.mu.Lock()
	defer iam.mu.Unlock()

	return &PerceptibleConfig{
		State:    iam.state,
		Leader:   iam.leader,
		Self:     iam.self,
		Critical: iam.mustCritical,
		Peers:    iam.peer,
	}
}

func (iam *InactivatedMonitor) SetConfig(config *PerceptibleConfig) error {
	iam.mu.Lock()
	defer iam.mu.Unlock()

	iam.state = config.State
	iam.leader = config.Leader
	iam.self = config.Self
	iam.peer = config.Peers
	iam.mustCritical = config.Critical

	return nil
}

func (iam *InactivatedMonitor) Perceive(id uint64, isConnected bool) {}

func (iam *InactivatedMonitor) IsCritical() bool {
	iam.mu.Lock()
	defer iam.mu.Unlock()

	return iam.mustCritical
}

func (iam *InactivatedMonitor) TryGetActivate() (Perceptible, bool) {
	if a, err := iam.activation(iam.logger, iam.GetConfig()); err != nil {
		iam.logger.Error("failed to activate", zap.Error(err), zap.String("substitute", "InactivatedMonitor"))
		// if error occurs, switch to sheltering mode
		iam.mustCritical = true
		return iam, false
	} else {
		return a, true
	}
}

func NewInactivatedMonitor(lg *zap.Logger, cfg *PerceptibleConfig, activation func(logger *zap.Logger, config *PerceptibleConfig) (Perceptible, error)) (*InactivatedMonitor, error) {
	ret := &InactivatedMonitor{logger: lg, activation: activation}
	if err := ret.SetConfig(cfg); err != nil {
		return nil, err
	} else {
		return ret, nil
	}
}
