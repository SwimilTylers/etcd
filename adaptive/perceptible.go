package adaptive

import "go.etcd.io/etcd/raft"

type PerceptibleConfig struct {
	// State. It can be Follower/Leader/Candidate
	State raft.StateType

	// Leader id. If not decided, it sets to raft.None
	Leader uint64

	// Self id. Use to distinguish self from others
	Self uint64

	// Initial status of Perceptible
	Critical bool

	// Peers will be nil if there is no update.
	Peers []uint64
}

// Perceptible perceives the connectivity of peers
type Perceptible interface {
	// GetConfig returns current PerceptibleConfig
	GetConfig() *PerceptibleConfig

	// SetConfig allows configuration update, including:
	// 1. Add/Remove peers
	// 2. Explicit status update
	// 3. Change leadership
	// To be mentioned, all internal states should reset before the update
	SetConfig(config *PerceptibleConfig) error

	// Perceive check the current connectivity
	Perceive(id uint64, isConnected bool)

	// IsCritical tells whether data persistence is compulsory
	// This involves the adaptive coordination mechanism
	IsCritical() bool
}

func SetPerceptibleState(p Perceptible, state raft.StateType) error {
	cfg := p.GetConfig()
	cfg.State = state
	return p.SetConfig(cfg)
}

func SetPerceptibleLeader(p Perceptible, leader uint64) error {
	cfg := p.GetConfig()
	cfg.Leader = leader
	return p.SetConfig(cfg)
}

func SetPerceptibleLeaderAndState(p Perceptible, leader uint64, state raft.StateType) error {
	cfg := p.GetConfig()
	cfg.Leader = leader
	cfg.State = state
	return p.SetConfig(cfg)
}
