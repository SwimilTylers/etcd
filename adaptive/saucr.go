package adaptive

import (
	"go.etcd.io/etcd/raft"
	"go.uber.org/zap"
)

type SAUCRMonitor struct {
	logger *zap.Logger

	leader uint64
	state  raft.StateType

	maxToken int

	peers       []uint64
	unconnected []bool
	reporter    []*TokenBucketReporter

	threshold int
}

func (sm *SAUCRMonitor) GetConfig() *PerceptibleConfig {
	return &PerceptibleConfig{
		State:    sm.state,
		Leader:   sm.leader,
		Critical: sm.IsCritical(),
		Peers:    sm.peers,
	}
}

func (sm *SAUCRMonitor) SetConfig(config *PerceptibleConfig) error {
	sm.leader = config.Leader
	sm.state = config.State

	if config.Peers != nil {
		sm.peers = config.Peers

		sm.unconnected = make([]bool, len(sm.peers))
		sm.reporter = make([]*TokenBucketReporter, len(sm.peers))
		for i := 0; i < len(sm.peers); i++ {
			// initialize TokenBucketReporter.
			sm.reporter[i] = &TokenBucketReporter{maxToken: sm.maxToken}
		}

		sm.threshold = len(sm.peers)/2 - 1
	}

	// initial connectivity defined by config
	// If critical, sm set all sm.unconnected items to 'false'
	err := sm.refreshWithOption(config.Critical)

	return err
}

func (sm *SAUCRMonitor) Perceive(id uint64, isConnected bool) {
	var index = sm.findIndex(id)
	if index != -1 {
		if isConnected {
			sm.reporter[index].Positive()
		} else {
			sm.reporter[index].Negative()
		}

		sm.unconnected[index] = !sm.reporter[index].Test()

	} else if sm.logger != nil {
		sm.logger.Warn(
			"SAUCRMonitor fail to perceive connectivity from unknown id",
			zap.Uint64("perceived-id", id),
		)
	}
}

func (sm *SAUCRMonitor) IsCritical() bool {
	if sm.state == raft.StateLeader {
		counter := 0
		for _, u := range sm.unconnected {
			if u {
				counter++
			}
		}
		return counter >= sm.threshold
	} else if sm.state == raft.StateFollower {
		counter := 0
		for _, u := range sm.unconnected {
			if u {
				counter++
			}
		}

		if sm.leader != raft.None {
			var leaderIdx = sm.findIndex(sm.leader)
			if leaderIdx != -1 {
				return sm.unconnected[leaderIdx] && counter >= sm.threshold
			}
		}
		return counter >= sm.threshold

	} else {
		return true
	}
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

func NewSAUCRMonitor(logger *zap.Logger, maxConnToken int, config *PerceptibleConfig) *SAUCRMonitor {
	ret := &SAUCRMonitor{logger: logger, maxToken: maxConnToken}
	_ = ret.SetConfig(config)
	return ret
}
