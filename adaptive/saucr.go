package adaptive

import (
	"bytes"
	"errors"
	"fmt"
	"go.etcd.io/etcd/raft"
	"go.uber.org/zap"
	"sync"
)

type SaucrMonitor struct {
	logger *zap.Logger

	mu sync.Mutex

	leader uint64
	self   uint64
	state  raft.StateType

	peers       []uint64
	unconnected []bool
	hbCounter   []HeartbeatCounter

	hbCounterFactory func() HeartbeatCounter

	threshold int

	mustCritical bool
}

func (sm *SaucrMonitor) String() string {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var buffer bytes.Buffer
	buffer.WriteString(
		fmt.Sprintf("[self=%04x,leader=%04x,state=%v][plen=%d,thresh=%d,must=%v]\n",
			sm.self, sm.leader, sm.state, len(sm.peers), sm.threshold, sm.mustCritical,
		),
	)

	for i := 0; i < len(sm.peers); i++ {
		buffer.WriteString(fmt.Sprintf("\t%04x[unconn=%v]: %s \n", sm.peers[i], sm.unconnected[i], sm.hbCounter[i]))
	}

	return buffer.String()
}

func (sm *SaucrMonitor) GetConfig() *PerceptibleConfig {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return &PerceptibleConfig{
		State:    sm.state,
		Leader:   sm.leader,
		Self:     sm.self,
		Critical: sm.evaluate(), // lazy evaluation
		Peers:    sm.peers,
	}
}

func (sm *SaucrMonitor) SetConfig(config *PerceptibleConfig) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if config.State == raft.StateLeader && config.Leader != config.Self {
		err := errors.New("incoherent leader settings")
		if sm.logger != nil {
			sm.logger.Fatal("SAUCR monitor config is illegal",
				zap.Uint64("monitor-self-id", sm.self),
				zap.Error(err),
			)
		}
		return err
	}

	sm.leader = config.Leader
	sm.self = config.Self

	// config.Critical is initialized implicitly

	if err := sm.refreshStateWithOption(config.State, config.Critical); err != nil {
		if sm.logger != nil {
			sm.logger.Fatal("SAUCR monitor is not properly refreshed",
				zap.Uint64("monitor-self-id", sm.self),
				zap.Error(err),
			)
		}
		return err
	}

	if err := sm.refreshPeersWithOption(config.Peers, config.Critical); err != nil {
		if sm.logger != nil {
			sm.logger.Fatal("SAUCR monitor is not properly refreshed",
				zap.Uint64("monitor-self-id", sm.self),
				zap.Error(err),
			)
		}
		return err
	}

	return nil
}

func (sm *SaucrMonitor) Perceive(id uint64, isConnected bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if id == sm.self {
		return
	}

	if sm.state != raft.StateLeader {
		return
	}

	var index = sm.findIndex(id)
	if index != -1 {
		if isConnected {
			sm.hbCounter[index].Positive()
		} else {
			sm.hbCounter[index].Negative()
		}

		sm.unconnected[index] = !sm.hbCounter[index].Report()

	} else if sm.logger != nil {
		sm.logger.Warn(
			"SAUCR monitor fail to perceive connectivity from unknown id",
			zap.Uint64("self-id", sm.self),
			zap.Uint64("perceived-id", id),
		)
	}
}

func (sm *SaucrMonitor) IsCritical() bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.evaluate()
}

func (sm *SaucrMonitor) refreshStateWithOption(state raft.StateType, isInitCritical bool) error {
	sm.state = state

	if state == raft.StateLeader {
		// leader should decide upon current connectivity
		sm.mustCritical = false
	} else if state == raft.StateFollower {
		sm.mustCritical = isInitCritical
	} else {
		sm.mustCritical = isInitCritical

		// is external mode switch legal ?
		if !isInitCritical {
			return errors.New("illegal refresh of the field 'mustCritical'")
		}
	}

	return nil
}

func (sm *SaucrMonitor) refreshPeersWithOption(peers []uint64, isInitCritical bool) error {
	if peers == nil || len(peers) == 0 {
		return errors.New("monitor is refreshed by empty peer list")
	} else if len(peers) < 3 {
		return errors.New("monitor is refreshed by peer list with length < 3")
	}

	if sm.peers == nil || len(sm.peers) != len(peers) {
		sm.unconnected = make([]bool, len(peers))
		sm.hbCounter = make([]HeartbeatCounter, len(peers))

		for i := 0; i < len(sm.hbCounter); i++ {
			sm.hbCounter[i] = sm.hbCounterFactory()
		}
	}

	sm.peers = peers

	for i := 0; i < len(sm.peers); i++ {
		sm.hbCounter[i].Init(isInitCritical)
		sm.unconnected[i] = isInitCritical
	}

	sm.threshold = (len(sm.peers) - 1) / 2

	return nil
}

func (sm *SaucrMonitor) findIndex(key uint64) int {
	for i, peer := range sm.peers {
		if peer == key {
			return i
		}
	}

	return -1
}

func (sm *SaucrMonitor) countUnconnectedEscapeSelf() int {
	counter := 0
	for i, u := range sm.unconnected {
		if u && sm.peers[i] != sm.self {
			counter++
		}
	}
	return counter
}

func (sm *SaucrMonitor) evaluate() bool {
	if sm.mustCritical {
		// if manually set mustCritical, it should blindly follows
		return true
	} else {
		// otherwise, monitor should decide on whether to be critical
		if sm.state == raft.StateLeader {
			// if it is a leader, check followers' connectivity
			return sm.countUnconnectedEscapeSelf() >= sm.threshold
		} else if sm.state == raft.StateFollower {
			// if it is a follower, check leader's validity

			if sm.leader != raft.None {
				// if leader is not overthrown, check its connectivity

				var leaderIdx = sm.findIndex(sm.leader)
				if leaderIdx != -1 {
					return sm.unconnected[leaderIdx]
				}
			}
		}

		return true
	}
}

func NewSaucrMonitor(logger *zap.Logger, hbCounterFactory func() HeartbeatCounter, config *PerceptibleConfig) (*SaucrMonitor, error) {
	ret := &SaucrMonitor{logger: logger, mu: sync.Mutex{}, hbCounterFactory: hbCounterFactory}
	if err := ret.SetConfig(config); err != nil {
		return nil, err
	} else {
		return ret, nil
	}
}
