package adaptive

import (
	"bytes"
	"errors"
	"fmt"
	"go.etcd.io/etcd/raft"
	"go.uber.org/zap"
	"sync"
)

type SAUCRMonitor struct {
	logger *zap.Logger

	mu sync.Mutex

	leader uint64
	self   uint64
	state  raft.StateType

	maxToken int

	peers       []uint64
	unconnected []bool
	reporter    []*TokenBucketReporter

	threshold int

	mustCritical bool
}

func (sm *SAUCRMonitor) String() string {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var buffer bytes.Buffer
	buffer.WriteString(
		fmt.Sprintf("[self=%04x,leader=%04x,state=%v][plen=%d,thresh=%d,must=%v]\n",
			sm.self, sm.leader, sm.state, len(sm.peers), sm.threshold, sm.mustCritical,
		),
	)

	for i := 0; i < len(sm.peers); i++ {
		buffer.WriteString(fmt.Sprintf("\t%04x[unconn=%v]: %s \n", sm.peers[i], sm.unconnected[i], sm.reporter[i]))
	}

	return buffer.String()
}

func (sm *SAUCRMonitor) MaxToken() int {
	return sm.maxToken
}

func (sm *SAUCRMonitor) SetMaxToken(maxToken int) {
	sm.maxToken = maxToken
}

func (sm *SAUCRMonitor) GetConfig() *PerceptibleConfig {
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

func (sm *SAUCRMonitor) SetConfig(config *PerceptibleConfig) error {
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

func (sm *SAUCRMonitor) Perceive(id uint64, isConnected bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if id == sm.self {
		return
	}

	var index = sm.findIndex(id)
	if index != -1 {
		if isConnected {
			sm.reporter[index].Positive()
		} else {
			sm.reporter[index].Negative()
		}

		sm.unconnected[index] = !sm.reporter[index].Report()

	} else if sm.logger != nil {
		sm.logger.Warn(
			"SAUCR monitor fail to perceive connectivity from unknown id",
			zap.Uint64("self-id", sm.self),
			zap.Uint64("perceived-id", id),
		)
	}
}

func (sm *SAUCRMonitor) IsCritical() bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.evaluate()
}

func (sm *SAUCRMonitor) refreshStateWithOption(state raft.StateType, isInitCritical bool) error {
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

func (sm *SAUCRMonitor) refreshPeersWithOption(peers []uint64, isInitCritical bool) error {
	if peers == nil || len(peers) == 0 {
		return errors.New("monitor is refreshed by empty peer list")
	} else if len(peers) < 3 {
		return errors.New("monitor is refreshed by peer list with length < 3")
	}

	sm.peers = peers

	sm.unconnected = make([]bool, len(sm.peers))
	sm.reporter = make([]*TokenBucketReporter, len(sm.peers))
	for i := 0; i < len(sm.peers); i++ {
		sm.reporter[i] = &TokenBucketReporter{maxToken: sm.maxToken}
		sm.unconnected[i] = isInitCritical
	}

	sm.threshold = (len(sm.peers) - 1) / 2

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

func (sm *SAUCRMonitor) countUnconnectedEscapeSelf() int {
	counter := 0
	for i, u := range sm.unconnected {
		if u && sm.peers[i] != sm.self {
			counter++
		}
	}
	return counter
}

func (sm *SAUCRMonitor) evaluate() bool {
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

func NewSAUCRMonitor(logger *zap.Logger, maxConnToken int, config *PerceptibleConfig) (*SAUCRMonitor, error) {
	ret := &SAUCRMonitor{logger: logger, mu: sync.Mutex{}, maxToken: maxConnToken}
	if err := ret.SetConfig(config); err != nil {
		return nil, err
	} else {
		return ret, nil
	}
}
