package adaptive

import (
	"go.etcd.io/etcd/raft"
	"math/rand"
	"testing"
)

func TestInitialization(t *testing.T) {
	var peerSize = 5
	var tokenSize = 5
	var nonThreat = tokenSize/2 - 1

	var config = &PerceptibleConfig{
		State:    raft.StateFollower,
		Leader:   raft.None,
		Critical: false,
		Peers:    GeneratePeers(peerSize),
	}

	// normal start
	machine := NewSAUCRMonitor(nil, tokenSize, config)

	if machine.IsCritical() {
		t.Fatal("INIT01: When config.Critical is false, SAUCR should be initialized as non-critical state")
	}

	// before leader is find
	// SC-I: most peers work normally
	for i, peer := range config.Peers {
		machine.Perceive(peer, true)
		if machine.IsCritical() {
			t.Errorf("INIT01-SCI: must stay non-critical, when i=%d\n", i)
		}
	}

	// SC-II: most peers work abnormally
	isDesc := false
	_ = machine.SetConfig(config) // refreshing
	for i, peer := range config.Peers {
		machine.Perceive(peer, false)
		if machine.IsCritical() && i < nonThreat {
			t.Errorf("INIT01-SCII: must stay non-critical, when i=%d\n", i)
			isDesc = true
		} else if !machine.IsCritical() && i >= nonThreat {
			t.Errorf("INIT01-SCII: must change to critical, when i=%d\n", i)
		}
	}
	if isDesc {
		t.Log("INIT01-SCII: when network deteriorates before find any leader")
		isDesc = false
	}

	// when find a leader
	oldLeader := config.Leader
	config.Leader = config.Peers[2]
	_ = machine.SetConfig(config)

	if machine.IsCritical() {
		t.Error("INIT02: When a leader is found, SAUCR should remain in non-critical state")
	}

	config.Leader = oldLeader

	// normal restart
	config.Critical = true
	_ = machine.SetConfig(config)

	if !machine.IsCritical() {
		t.Fatal("INI03: When config.Critical is true, SAUCR should be initialized as critical state")
	}

	// before leader is find
	// SC-I: most peers work normally
	for i, peer := range config.Peers {
		machine.Perceive(peer, false)
		if machine.IsCritical() {
			t.Errorf("INIT03-SCI: must stay critical, when i=%d\n", i)
		}
	}

	// SC-II: most peers work abnormally
	isDesc = false
	_ = machine.SetConfig(config) // refreshing
	for i, peer := range config.Peers {
		machine.Perceive(peer, true)
		if !machine.IsCritical() && i < peerSize-nonThreat {
			t.Errorf("INIT03-SCII: must stay critical, when i=%d\n", i)
			isDesc = true
		} else if machine.IsCritical() && i >= peerSize-nonThreat {
			t.Errorf("INIT03-SCII: must change to non-critical, when i=%d\n", i)
		}
	}
	if isDesc {
		t.Log("INIT03-SCII: when network improves before find any leader")
		isDesc = false
	}

	// when find a leader
	oldLeader = config.Leader
	config.Leader = config.Peers[2]

	if !machine.IsCritical() {
		t.Error("INIT04: Even if a leader is defined, SAUCR should run in non-critical state")
	}

	config.Leader = oldLeader
}

func GeneratePeers(peerSize int) []uint64 {
	peers := make([]uint64, peerSize)
	for i := 0; i < peerSize; i++ {
		peers[i] = rand.Uint64()
	}
	return peers
}
