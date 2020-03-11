package adaptive

import (
	"go.etcd.io/etcd/raft"
	"math/rand"
	"testing"
)

func TestInitializationNonCritical(t *testing.T) {
	var peerSize = 5
	var tokenSize = 2

	var selfIdx = 0
	var leaderIdx = 2

	var config = &PerceptibleConfig{
		State:    raft.StateFollower,
		Leader:   raft.None,
		Critical: false,
		Peers:    GeneratePeers(peerSize),
	}

	config.Self = config.Peers[selfIdx]

	// 01: non-critical start
	machine, _ := NewSAUCRMonitor(nil, tokenSize, config)

	if !machine.IsCritical() {
		t.Fatal("01: stay critical before any leader is chosen")
	}

	// 01-SCI: well-connected
	// follower's mode depends on leader's status, which is not found yet
	for _, peer := range config.Peers {
		machine.Perceive(peer, true)

		if !machine.IsCritical() {
			t.Error("01-SCI: must stay critical, since no leader is found")
		}
	}

	// 01-SCII: ill-connected
	// follower's mode depends on leader's status, which is not found yet
	_ = machine.SetConfig(config) // refreshing
	for i, peer := range config.Peers {
		if i == selfIdx {
			continue
		}

		machine.Perceive(peer, false)

		if !machine.IsCritical() {
			t.Error("01-SCII: must stay critical, since no leader is found")
		}
	}

	// 02: find a leader
	_ = machine.SetConfig(config) // refreshing

	st0 := machine.IsCritical()

	oldLeader := config.Leader
	config.Leader = config.Peers[leaderIdx]
	_ = machine.SetConfig(config) // refreshing

	st1 := machine.IsCritical()

	if !(st0 == true && st1 == false) {
		t.Errorf("02: After a leader is found, isCritical should change true -> false, rather than %v -> %v/n", st0, st1)
	}

	config.Leader = oldLeader

	// 03: become a leader
	_ = machine.SetConfig(config) // refreshing

	st0 = machine.IsCritical()

	oldLeader = config.Leader
	oldState := config.State

	config.Leader = config.Peers[selfIdx]
	config.State = raft.StateLeader

	_ = machine.SetConfig(config) // refreshing

	st1 = machine.IsCritical()

	if !(st0 == true && st1 == false) {
		t.Errorf("03: After becoming a leader, isCritical should change true -> false, rather than %v -> %v/n", st0, st1)
	}

	config.Leader = oldLeader
	config.State = oldState

	// 04: become a candidate
	_ = machine.SetConfig(config) // refreshing

	st0 = machine.IsCritical()

	oldLeader = config.Leader
	oldState = config.State

	config.Leader = config.Peers[selfIdx]
	config.State = raft.StateCandidate

	_ = machine.SetConfig(config) // refreshing

	st1 = machine.IsCritical()

	if !(st0 == true && st1 == true) {
		t.Errorf("04: After becoming a candidate, isCritical should change true -> true, rather than %v -> %v", st0, st1)
	}

	config.Leader = oldLeader
	config.State = oldState

}

func TestInitializationCritical(t *testing.T) {
	var peerSize = 5
	var tokenSize = 2

	var selfIdx = 0
	var leaderIdx = 2

	var config = &PerceptibleConfig{
		State:    raft.StateFollower,
		Leader:   raft.None,
		Critical: true,
		Peers:    GeneratePeers(peerSize),
	}

	config.Self = config.Peers[selfIdx]

	// 01: non-critical start
	machine, _ := NewSAUCRMonitor(nil, tokenSize, config)

	if !machine.IsCritical() {
		t.Fatal("01: When config.Critical is false, SAUCR should be initialized as critical state")
	}

	// 01-SCI: well-connected
	// follower's mode depends on leader's status, which is not found yet
	for _, peer := range config.Peers {
		machine.Perceive(peer, false)

		if !machine.IsCritical() {
			t.Error("01-SCI: must stay critical, since no leader is found")
		}
	}

	// 01-SCII: ill-connected
	// follower's mode depends on leader's status, which is not found yet
	_ = machine.SetConfig(config) // refreshing
	for i, peer := range config.Peers {
		if i == selfIdx {
			continue
		}

		machine.Perceive(peer, true)

		if !machine.IsCritical() {
			t.Error("01-SCII: must stay critical, since no leader is found")
		}
	}

	// 02: find a leader
	oldLeader := config.Leader
	config.Leader = config.Peers[leaderIdx]
	_ = machine.SetConfig(config) // refreshing

	if !machine.IsCritical() {
		t.Error("02: After a leader is found, isCritical should stay true before explicit switch")
	}

	config.Leader = oldLeader

	// 02-SCI: receive a explicit non-critical
	oldLeader = config.Leader
	oldCritical := config.Critical

	config.Leader = config.Peers[leaderIdx]
	config.Critical = false
	_ = machine.SetConfig(config) // refreshing

	if machine.IsCritical() {
		t.Error("02-SCI: After a leader is found, isCritical should respond to explicit switch")
	}

	config.Leader = oldLeader
	config.Critical = oldCritical

	// 02-SCII: receive a explicit non-critical
	oldLeader = config.Leader
	oldCritical = config.Critical

	config.Leader = config.Peers[leaderIdx]
	config.Critical = true
	_ = machine.SetConfig(config) // refreshing

	if !machine.IsCritical() {
		t.Error("02-SCII: After a leader is found, isCritical should respond to explicit switch")
	}

	config.Leader = oldLeader
	config.Critical = oldCritical

	// 03: become a leader
	_ = machine.SetConfig(config) // refreshing

	st0 := machine.IsCritical()

	oldLeader = config.Leader
	oldState := config.State
	oldCritical = config.Critical

	config.Leader = config.Peers[selfIdx]
	config.State = raft.StateLeader
	config.Critical = false

	_ = machine.SetConfig(config) // refreshing

	st1 := machine.IsCritical()

	if !(st0 == true && st1 == false) {
		t.Errorf("03: After becoming a leader, isCritical should change true -> false, rather than %v -> %v", st0, st1)
	}

	config.Leader = oldLeader
	config.State = oldState
	config.Critical = oldCritical

	// 03-LC: become a leader, but stay critical
	oldLeader = config.Leader
	oldState = config.State
	oldCritical = config.Critical

	config.Leader = config.Peers[selfIdx]
	config.State = raft.StateLeader
	config.Critical = true

	_ = machine.SetConfig(config) // refreshing

	if !(machine.IsCritical()) {
		t.Error("03-LC: though becomes a leader, it should stay critical when network is not well")
	}

	config.Leader = oldLeader
	config.State = oldState
	config.Critical = oldCritical

	// 04: become a candidate
	_ = machine.SetConfig(config) // refreshing

	st0 = machine.IsCritical()

	oldLeader = config.Leader
	oldState = config.State

	config.Leader = config.Peers[selfIdx]
	config.State = raft.StateCandidate

	_ = machine.SetConfig(config) // refreshing

	st1 = machine.IsCritical()

	if !(st0 == true && st1 == true) {
		t.Errorf("04: After becoming a candidate, isCritical should change true -> true, rather than %v -> %v", st0, st1)
	}

	config.Leader = oldLeader
	config.State = oldState
}

func TestLeaderDetection(t *testing.T) {
	var peerSize = 5
	var tokenSize = 2
	var threshold = (peerSize - 1) / 2

	var selfIdx = 0

	var config = &PerceptibleConfig{
		State:    raft.StateLeader,
		Critical: false,
		Peers:    GeneratePeers(peerSize),
	}

	config.Self = config.Peers[selfIdx]
	config.Leader = config.Self

	// normal start
	machine, _ := NewSAUCRMonitor(nil, tokenSize, config)

	if machine.IsCritical() {
		t.Fatal("saucr should be initialized in non-critical mode")
	}

	machine.Perceive(config.Self, false)

	if machine.IsCritical() {
		t.Error("self-disconnection should not perform")
	}

	var failed = 0
	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, false)
		failed++

		if failed < threshold {
			if machine.IsCritical() {
				t.Errorf("when failed=%d, leader should run in non-critical mode", failed)
			}
		} else {
			if !machine.IsCritical() {
				t.Errorf("when failed=%d, leader should run in critical mode", failed)
			}
		}
	}

	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, true)
		failed--

		if failed < threshold {
			if machine.IsCritical() {
				t.Errorf("when failed=%d, leader should run in non-critical mode", failed)
			}
		} else {
			if !machine.IsCritical() {
				t.Errorf("when failed=%d, leader should run in critical mode", failed)
			}
		}
	}

	config.Critical = true

	_ = machine.SetConfig(config)

	if !machine.IsCritical() {
		t.Fatal("when leader launch a mode switch, it should transfer into critical mode")
	} else if machine.mustCritical {
		t.Fatal("leader.mustCritical must set to false")
	}

	machine.Perceive(config.Self, false)

	failed = peerSize - 1

	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, true)
		failed--

		if failed < threshold {
			if machine.IsCritical() {
				t.Errorf("when failed=%d, leader should run in non-critical mode", failed)
			}
		} else {
			if !machine.IsCritical() {
				t.Errorf("when failed=%d, leader should run in critical mode", failed)
			}
		}
	}

	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, false)
		failed++

		if failed < threshold {
			if machine.IsCritical() {
				t.Errorf("when failed=%d, leader should run in non-critical mode", failed)
			}
		} else {
			if !machine.IsCritical() {
				t.Errorf("when failed=%d, leader should run in critical mode", failed)
			}
		}
	}
}

func TestFollowerDetection(t *testing.T) {
	var peerSize = 5
	var tokenSize = 2

	var selfIdx = 0
	var leaderIdx = 2

	var config = &PerceptibleConfig{
		State:    raft.StateFollower,
		Critical: false,
		Peers:    GeneratePeers(peerSize),
	}

	config.Self = config.Peers[selfIdx]
	config.Leader = config.Peers[leaderIdx]

	// normal start
	machine, _ := NewSAUCRMonitor(nil, tokenSize, config)

	if machine.IsCritical() {
		t.Fatal("saucr should be initialized in non-critical mode")
	}

	machine.Perceive(config.Self, false)

	if machine.IsCritical() {
		t.Error("self-disconnection should not perform")
	}

	var failed = false
	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, false)

		if peer == config.Leader {
			failed = true
		}

		if failed {
			if !machine.IsCritical() {
				t.Error("when leader failed, follower should run in critical mode")
			}
		} else {
			if machine.IsCritical() {
				t.Error("before any explicit switch or leader failure, follower should run in non-critical mode")
			}
		}
	}

	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, true)

		if peer == config.Leader {
			failed = false
		}

		if failed {
			if !machine.IsCritical() {
				t.Error("when leader failed, follower should run in critical mode")
			}
		} else {
			if machine.IsCritical() {
				t.Errorf("after leader reconnected, follower should run in non-critical mode")
			}
		}
	}

	config.Critical = true

	// normal start
	_ = machine.SetConfig(config)

	if !machine.IsCritical() {
		t.Fatal("saucr should be initialized in critical mode")
	}

	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, false)

		if !machine.IsCritical() {
			t.Error("Candidate should run in critical mode")
		}
	}

	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, true)

		if !machine.IsCritical() {
			t.Error("Candidate should run in critical mode")
		}
	}
}

func TestCandidateDetection(t *testing.T) {
	var peerSize = 5
	var tokenSize = 2

	var selfIdx = 0

	var config = &PerceptibleConfig{
		State:    raft.StateCandidate,
		Critical: false,
		Peers:    GeneratePeers(peerSize),
	}

	config.Self = config.Peers[selfIdx]
	config.Leader = raft.None

	// normal start
	machine, err := NewSAUCRMonitor(nil, tokenSize, config)

	if err == nil {
		t.Fatal("instantiation should detect illegal field:", err)
	}

	config.Critical = true

	// normal start
	machine, _ = NewSAUCRMonitor(nil, tokenSize, config)

	if !machine.IsCritical() {
		t.Fatal("saucr should be initialized in critical mode")
	}

	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, false)

		if !machine.IsCritical() {
			t.Error("Candidate should run in critical mode")
		}
	}

	for _, peer := range config.Peers {
		if peer == config.Self {
			continue
		}

		machine.Perceive(peer, true)

		if !machine.IsCritical() {
			t.Error("Candidate should run in critical mode")
		}
	}
}

func TestModeSwitch(t *testing.T) {
	machine, _ := NewSAUCRMonitor(nil, 2, &PerceptibleConfig{
		State:    raft.StateLeader,
		Critical: false,
		Peers:    GeneratePeers(5),
	})

	machine.self = machine.peers[0]
	machine.leader = machine.peers[0]

	if machine.IsCritical() {
		t.Fatal("leader should initialize as non-critical")
	}

	oldConfig := machine.GetConfig()
	_ = machine.SetConfig(&PerceptibleConfig{
		State:    oldConfig.State,
		Leader:   oldConfig.Leader,
		Self:     oldConfig.Self,
		Critical: true,
		Peers:    oldConfig.Peers,
	})

	if !machine.IsCritical() {
		t.Fatal("leader should switch to critical")
	} else if machine.mustCritical {
		t.Fatal("leader.mustCritical should set false")
	}

	oldConfig = machine.GetConfig()
	_ = machine.SetConfig(&PerceptibleConfig{
		State:    oldConfig.State,
		Leader:   oldConfig.Leader,
		Self:     oldConfig.Self,
		Critical: false,
		Peers:    oldConfig.Peers,
	})

	if machine.IsCritical() {
		t.Fatal("leader should switch to non-critical")
	}

	machine, _ = NewSAUCRMonitor(nil, 2, &PerceptibleConfig{
		State:    raft.StateFollower,
		Critical: false,
		Peers:    GeneratePeers(5),
	})

	machine.self = machine.peers[0]
	machine.leader = machine.peers[2]

	if machine.IsCritical() {
		t.Fatal("leader should initialize as non-critical")
	}

	oldConfig = machine.GetConfig()
	_ = machine.SetConfig(&PerceptibleConfig{
		State:    oldConfig.State,
		Leader:   oldConfig.Leader,
		Self:     oldConfig.Self,
		Critical: true,
		Peers:    oldConfig.Peers,
	})

	if !machine.IsCritical() {
		t.Fatal("leader should switch to critical")
	}

	oldConfig = machine.GetConfig()
	_ = machine.SetConfig(&PerceptibleConfig{
		State:    oldConfig.State,
		Leader:   oldConfig.Leader,
		Self:     oldConfig.Self,
		Critical: false,
		Peers:    oldConfig.Peers,
	})

	if machine.IsCritical() {
		t.Fatal("leader should switch to non-critical")
	}

	machine.Perceive(machine.leader, false)

	if !machine.IsCritical() {
		t.Fatal("leader should switch to critical")
	}
}

func GeneratePeers(peerSize int) []uint64 {
	peers := make([]uint64, peerSize)

	already := make(map[uint64]struct{})

	// avoid raft.None
	already[raft.None] = struct{}{}

	for i := 0; i < peerSize; i++ {
		peers[i] = rand.Uint64()

		// avoid generate raft.None and conflict name

		for {
			if _, ok := already[peers[i]]; ok {
				peers[i] = rand.Uint64()
			} else {
				already[peers[i]] = struct{}{}
				break
			}
		}
	}
	return peers
}
