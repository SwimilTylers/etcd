package adaptive

import (
	"go.etcd.io/etcd/raft/raftpb"
	"time"
)

var DefaultStrategy = &PersistentStrategy{
	Fsync:             false,
	MaxLocalCacheSize: 50000,
	CachePreserveTime: 30 * time.Second,
}

type PersistentConfig struct {
	Strategy *PersistentStrategy
	Remotes  []*PersistentRemoteDescriptor
}

type PersistentStrategy struct {
	Fsync             bool
	MaxLocalCacheSize int
	CachePreserveTime time.Duration
}

type PersistentRemoteDescriptor struct {
}

// PersistentManager wraps Storage and keeps track on each persistent ops
type PersistentManager interface {
	Save(st raftpb.HardState, ents []raftpb.Entry) error
	SaveSnap(snap raftpb.Snapshot) error
	Close() error

	UnPersisted() bool
	Flush() error

	GetConfig() *PersistentConfig
	SetConfig(cfg *PersistentConfig) error

	GetStrategy() *PersistentStrategy
	SetStrategy(strategy *PersistentStrategy) error

	AddRemoteDisk(desc *PersistentRemoteDescriptor) error
	RemoveRemoteDisk(desc *PersistentRemoteDescriptor) error
}

func SetPersistentFsync(manager PersistentManager, fsync bool) error {
	strategy := manager.GetStrategy()
	strategy.Fsync = fsync
	return manager.SetStrategy(strategy)
}
