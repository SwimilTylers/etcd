package adaptive

import "go.uber.org/zap"

type SAUCRMonitor struct {
	logger *zap.Logger

	isLeader uint64

	maxToken int

	peers       []uint64
	unconnected []bool
	reporter    []*ConnectionReporter

	threshold int
}

func (sm *SAUCRMonitor) GetConfig() *PerceptibleConfig {
	return &PerceptibleConfig{
		Leader:   sm.isLeader,
		Critical: sm.IsCritical(),
		Peers:    sm.peers,
	}
}

func (sm *SAUCRMonitor) SetConfig(config *PerceptibleConfig) error {
	sm.isLeader = config.Leader

	if config.Peers != nil {
		sm.peers = config.Peers

		sm.unconnected = make([]bool, len(sm.peers))
		sm.reporter = make([]*ConnectionReporter, len(sm.peers))
		for i := 0; i < len(sm.peers); i++ {
			sm.reporter[i] = &ConnectionReporter{maxToken: sm.maxToken}
		}
	}

	return sm.refreshWithOption(config.Critical)
}

func (sm *SAUCRMonitor) Perceive(id uint64, isConnected bool) {
	var index = sm.findIndex(id)
	if index != -1 {
		if isConnected {
			sm.reporter[index].Positive()
		} else {
			sm.reporter[index].Negative()
		}

		sm.unconnected[index] = !sm.reporter[index].Gather()

	} else {
		sm.logger.Warn(
			"SAUCRMonitor fail to perceive connectivity from unknown id",
			zap.Uint64("perceived-id", id),
		)
	}
}

func (sm *SAUCRMonitor) IsCritical() bool {
	counter := 0
	for _, u := range sm.unconnected {
		if u {
			counter++
		}
	}
	return counter >= sm.threshold
}

func (sm *SAUCRMonitor) refreshWithOption(isInitCritical bool) error {
	for i := 0; i < len(sm.unconnected); i++ {
		sm.unconnected[i] = isInitCritical
	}
	return nil
}

func (sm *SAUCRMonitor) findIndex(key uint64) int {
	for i, peer := range sm.peers {
		if peer == key {
			return i
		}
	}

	return -1
}

func NewPeerMonitor(logger *zap.Logger, maxConnToken int, config *PerceptibleConfig) *SAUCRMonitor {
	ret := &SAUCRMonitor{logger: logger, maxToken: maxConnToken}
	_ = ret.SetConfig(config)
	return ret
}
