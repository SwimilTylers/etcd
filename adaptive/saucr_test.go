package adaptive

import (
	"go.etcd.io/etcd/raft"
	"math/rand"
	"strings"
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
	machine, _ := NewSaucrMonitor(nil, NewSimpleBucketCounterFactory(tokenSize), config)

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
	machine, _ := NewSaucrMonitor(nil, NewSimpleBucketCounterFactory(tokenSize), config)

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
	machine, _ := NewSaucrMonitor(nil, NewSimpleBucketCounterFactory(tokenSize), config)

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
	t.Skip("According to etcd-notes-saucr-implementation.md,",
		"followers can not detect the leader's connectivity unless through the RaftState change (F->C).",
	)

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
	machine, _ := NewSaucrMonitor(nil, NewSimpleBucketCounterFactory(tokenSize), config)

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
	machine, err := NewSaucrMonitor(nil, NewSimpleBucketCounterFactory(tokenSize), config)

	if err == nil {
		t.Fatal("instantiation should detect illegal field:", err)
	}

	config.Critical = true

	// normal start
	machine, _ = NewSaucrMonitor(nil, NewSimpleBucketCounterFactory(tokenSize), config)

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
	machine, _ := NewSaucrMonitor(nil, NewSimpleBucketCounterFactory(2), &PerceptibleConfig{
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

	machine, _ = NewSaucrMonitor(nil, NewSimpleBucketCounterFactory(2), &PerceptibleConfig{
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
}

func TestStateSwitch(t *testing.T) {
	peers := GeneratePeers(5)
	nLeaderCfg := &PerceptibleConfig{
		State:    raft.StateLeader,
		Leader:   peers[0],
		Self:     peers[0],
		Critical: false,
		Peers:    peers,
	}

	uLeaderCfg := &PerceptibleConfig{
		State:    raft.StateLeader,
		Leader:   peers[0],
		Self:     peers[0],
		Critical: true,
		Peers:    peers,
	}

	nFollowerCfg := &PerceptibleConfig{
		State:    raft.StateFollower,
		Leader:   peers[3],
		Self:     peers[0],
		Critical: false,
		Peers:    peers,
	}

	uFollowerCfg := &PerceptibleConfig{
		State:    raft.StateFollower,
		Leader:   peers[3],
		Self:     peers[0],
		Critical: true,
		Peers:    peers,
	}

	uCandidateCfg := &PerceptibleConfig{
		State:    raft.StateCandidate,
		Leader:   raft.None,
		Self:     peers[0],
		Critical: true,
		Peers:    peers,
	}

	// NL:
	// 1. NL -> UL
	// 2. NL -> NF

	machine, _ := NewSaucrMonitor(nil, CautiousHbCounterFactory, nLeaderCfg)
	if machine.IsCritical() {
		t.Error("NL initialization failed")
	}
	SaucrSeqFailureChecker(machine, SaucrSeqMonotonicChangeExpectation(machine.self, machine.leader, machine.peers, 3, "fail"), t)

	machine, _ = NewSaucrMonitor(nil, CautiousHbCounterFactory, nLeaderCfg)
	if machine.IsCritical() {
		t.Error("NL initialization failed")
	}
	if err := SetPerceptibleLeaderAndState(machine, peers[2], raft.StateFollower); err != nil {
		t.Fatal("NL->NF failed:", err)
	}
	if machine.IsCritical() {
		t.Error("NL->NF dysfunctional:", "must stay normal")
	}

	// UL:
	// 1. UL -> NL
	// 2. UL -> UF

	machine, _ = NewSaucrMonitor(nil, CautiousHbCounterFactory, uLeaderCfg)
	if !machine.IsCritical() {
		t.Error("NL initialization failed")
	}
	SaucrSeqRecoveryChecker(machine, SaucrSeqMonotonicChangeExpectation(machine.self, machine.leader, machine.peers, 4, "recover"), t)

	machine, _ = NewSaucrMonitor(nil, CautiousHbCounterFactory, uLeaderCfg)
	if !machine.IsCritical() {
		t.Error("UL initialization failed")
	}
	if err := SetPerceptibleLeaderAndState(machine, peers[2], raft.StateFollower); err != nil {
		t.Fatal("UL->UF failed:", err)
	}
	if !machine.IsCritical() {
		t.Error("UL->UF dysfunctional:", "must stay critical")
	}

	// NF:
	// 1. NF -> C
	// 2. NF -> UF

	machine, _ = NewSaucrMonitor(nil, CautiousHbCounterFactory, nFollowerCfg)
	if machine.IsCritical() {
		t.Error("NF initialization failed")
	}
	if err := SetPerceptibleCriticalAndState(machine, true, raft.StateCandidate); err != nil {
		t.Fatal("NF->C failed:", err)
	}
	if !machine.IsCritical() {
		t.Error("NF->C dysfunctional:", "must be critical")
	}

	machine, _ = NewSaucrMonitor(nil, CautiousHbCounterFactory, nFollowerCfg)
	if err := SetPerceptibleCritical(machine, true); err != nil {
		t.Fatal("NF->UF failed:", err)
	}
	if !machine.IsCritical() {
		t.Error("NF->UF dysfunctional:", "switch to critical after SAUCR_SHELTERING")
	}

	// UF:
	// 1. UF -> C
	// 2. UF -> NF

	machine, _ = NewSaucrMonitor(nil, CautiousHbCounterFactory, uFollowerCfg)
	if !machine.IsCritical() {
		t.Error("UF initialization failed")
	}
	if err := SetPerceptibleCriticalAndState(machine, true, raft.StateCandidate); err != nil {
		t.Fatal("UF->C failed:", err)
	}
	if !machine.IsCritical() {
		t.Error("UF->C dysfunctional:", "must be critical")
	}

	machine, _ = NewSaucrMonitor(nil, CautiousHbCounterFactory, uFollowerCfg)
	if err := SetPerceptibleCritical(machine, false); err != nil {
		t.Fatal("UF->NF failed:", err)
	}
	if machine.IsCritical() {
		t.Error("UF->NF dysfunctional:", "switch to normal after SAUCR_NORMAL")
	}

	// C:
	// 1. C -> UL
	// 2. C -> UF

	machine, _ = NewSaucrMonitor(nil, CautiousHbCounterFactory, uCandidateCfg)
	if !machine.IsCritical() {
		t.Error("C initialization failed")
	}
	if err := SetPerceptibleLeaderAndState(machine, machine.self, raft.StateLeader); err != nil {
		t.Fatal("C->UL failed:", err)
	}
	if !machine.IsCritical() {
		t.Error("C->UL dysfunctional:", "must stay critical")
	}

	machine, _ = NewSaucrMonitor(nil, CautiousHbCounterFactory, uCandidateCfg)
	if err := SetPerceptibleState(machine, raft.StateFollower); err != nil {
		t.Fatal("C->UF failed:", err)
	}
	if !machine.IsCritical() {
		t.Error("C->UF dysfunctional:", "must stay critical")
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

func SaucrSeqMonotonicChangeExpectation(self uint64, leader uint64, peers []uint64, b int, direction string) [][]bool {
	if self != leader {
		return nil
	}

	expected := make([][]bool, len(peers))

	switch strings.ToLower(direction) {
	case "fail":
		healthy := make([]bool, b+1)
		for i := 0; i < b+1; i++ {
			healthy[i] = false
		}

		ill := make([]bool, b+1)
		for i := 0; i < b+1; i++ {
			if i < b-1 {
				ill[i] = false
			} else {
				ill[i] = true
			}
		}

		dead := make([]bool, b+1)
		for i := 0; i < b+1; i++ {
			dead[i] = true
		}

		failed := 0

		for i := 0; i < len(expected); i++ {
			if peers[i] != self {
				failed++
				if failed < (len(peers)-1)/2 {
					expected[i] = healthy
				} else if failed == (len(peers)-1)/2 {
					expected[i] = ill
				} else {
					expected[i] = dead
				}
			} else {
				expected[i] = []bool{}
			}
		}
		return expected
	case "recover":
		healthy := make([]bool, b+1)
		for i := 0; i < b+1; i++ {
			healthy[i] = false
		}

		recovered := make([]bool, b+1)
		for i := 0; i < b+1; i++ {
			if i < b-1 {
				recovered[i] = true
			} else {
				recovered[i] = false
			}
		}

		dead := make([]bool, b+1)
		for i := 0; i < b+1; i++ {
			dead[i] = true
		}

		failed := len(peers) - 1

		for i := 0; i < len(expected); i++ {
			if peers[i] != self {
				failed--
				if failed < (len(peers)-1)/2-1 {
					expected[i] = healthy
				} else if failed == (len(peers)-1)/2-1 {
					expected[i] = recovered
				} else {
					expected[i] = dead
				}
			} else {
				expected[i] = []bool{}
			}
		}
		return expected
	default:
		return nil
	}
}

func SaucrSeqFailureChecker(monitor *SaucrMonitor, expected [][]bool, t *testing.T) {
	for i := 0; i < len(expected); i++ {
		for j := 0; j < len(expected[i]); j++ {
			monitor.Perceive(monitor.peers[i], false)
			if monitor.IsCritical() != expected[i][j] {
				t.Error("saucr seqFailure check error", "@(", i, j, "):", monitor.IsCritical(), "/", expected[i][j])
				return
			}
		}
	}
}

func SaucrSeqRecoveryChecker(monitor *SaucrMonitor, expected [][]bool, t *testing.T) {
	for i := 0; i < len(expected); i++ {
		for j := 0; j < len(expected[i]); j++ {
			monitor.Perceive(monitor.peers[i], false)
			monitor.Perceive(monitor.peers[i], true)
			if monitor.IsCritical() != expected[i][j] {
				t.Error("saucr seqRecovery check error", "@(", i, j, "):", monitor.IsCritical(), "/", expected[i][j])
				return
			}
		}
	}
}
