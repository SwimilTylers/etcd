package etcdserver

import (
	"errors"
	"go.etcd.io/etcd/adaptive"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"sync"
	"time"
)

var emptyState = raftpb.HardState{}

type LocalCachedDisk struct {
	logger *zap.Logger

	mu sync.Mutex

	// Fsync is a signal that decide whether to cache some persistent operations
	//
	// If it sets to true, cache will not allowed and previously cached items
	// will persist as well
	fsync bool

	// disk plays a key role in persistence
	disk Storage

	// local caches
	cachedHardState raftpb.HardState
	cachedEntries   []raftpb.Entry

	maxLocalCacheSize int

	cachePreserveTime time.Duration

	cachePreserve <-chan time.Time
}

func (lcd *LocalCachedDisk) Flush() error {
	if len(lcd.cachedEntries) != 0 {
		lcd.cachePreserve = nil

		err := lcd.disk.Save(lcd.cachedHardState, lcd.cachedEntries)

		lcd.cachedEntries = lcd.cachedEntries[:0]
		lcd.cachedHardState = emptyState

		return err
	} else {
		return nil
	}
}

func (lcd *LocalCachedDisk) GetConfig() *adaptive.PersistentConfig {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	return &adaptive.PersistentConfig{
		Strategy: &adaptive.PersistentStrategy{
			Fsync:             lcd.fsync,
			MaxLocalCacheSize: lcd.maxLocalCacheSize,
			CachePreserveTime: lcd.cachePreserveTime,
		},
		Remotes: nil,
	}
}

func (lcd *LocalCachedDisk) SetConfig(cfg *adaptive.PersistentConfig) error {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	if cfg.Remotes != nil {
		return errors.New("remote disk is not supported")
	}

	if cfg.Strategy != nil {
		if cfg.Strategy.MaxLocalCacheSize < 0 {
			return errors.New("illegal configuration")
		}

		lcd.fsync = cfg.Strategy.Fsync
		lcd.maxLocalCacheSize = cfg.Strategy.MaxLocalCacheSize
		lcd.cachePreserveTime = cfg.Strategy.CachePreserveTime

		if lcd.maxLocalCacheSize > cap(lcd.cachedEntries) {
			newCache := make([]raftpb.Entry, len(lcd.cachedEntries), lcd.maxLocalCacheSize+1)
			copy(newCache, lcd.cachedEntries)
			lcd.cachedEntries = newCache
		}
	}

	return nil
}

func (lcd *LocalCachedDisk) GetStrategy() *adaptive.PersistentStrategy {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	return &adaptive.PersistentStrategy{
		Fsync:             lcd.fsync,
		MaxLocalCacheSize: lcd.maxLocalCacheSize,
		CachePreserveTime: lcd.cachePreserveTime,
	}
}

func (lcd *LocalCachedDisk) SetStrategy(strategy *adaptive.PersistentStrategy) error {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	if strategy.MaxLocalCacheSize < 0 {
		return errors.New("illegal configuration")
	}

	lcd.fsync = strategy.Fsync
	lcd.maxLocalCacheSize = strategy.MaxLocalCacheSize
	lcd.cachePreserveTime = strategy.CachePreserveTime

	if lcd.maxLocalCacheSize > cap(lcd.cachedEntries) {
		newCache := make([]raftpb.Entry, len(lcd.cachedEntries), lcd.maxLocalCacheSize+1)
		copy(newCache, lcd.cachedEntries)
		lcd.cachedEntries = newCache
	}

	return nil
}

func (lcd *LocalCachedDisk) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	curLen := len(lcd.cachedEntries)
	newLen := len(ents)

	select {
	case <-lcd.cachePreserve:
		send := make([]raftpb.Entry, curLen+newLen)
		copy(send, lcd.cachedEntries)
		copy(send[curLen:], ents)

		lcd.cachePreserve = nil

		err := lcd.disk.Save(st, send)

		// clear cache
		lcd.cachedEntries = lcd.cachedEntries[:0]
		lcd.cachedHardState = emptyState

		return err
	default:
		if !lcd.fsync && curLen+newLen < lcd.maxLocalCacheSize {
			lcd.cachedEntries = lcd.cachedEntries[:curLen+newLen]
			copy(lcd.cachedEntries[curLen:], ents)
			lcd.cachedHardState = st

			if lcd.cachePreserve != nil {
				lcd.cachePreserve = time.After(lcd.cachePreserveTime)
			}

			return nil
		} else {
			send := make([]raftpb.Entry, curLen+newLen)
			copy(send, lcd.cachedEntries)
			copy(send[curLen:], ents)

			lcd.cachePreserve = nil

			err := lcd.disk.Save(st, send)

			// clear cache
			lcd.cachedEntries = lcd.cachedEntries[:0]
			lcd.cachedHardState = emptyState

			return err
		}
	}
}

func (lcd *LocalCachedDisk) SaveSnap(snap raftpb.Snapshot) error {
	return lcd.disk.SaveSnap(snap)
}

func (lcd *LocalCachedDisk) Close() error {
	return lcd.disk.Close()
}

func (lcd *LocalCachedDisk) AddRemoteDisk(desc *adaptive.PersistentRemoteDescriptor) error {
	return errors.New("unsupported operation")
}

func (lcd *LocalCachedDisk) RemoveRemoteDisk(desc *adaptive.PersistentRemoteDescriptor) error {
	return errors.New("unsupported operation")
}

func NewLocalDisk(logger *zap.Logger, disk Storage, config *adaptive.PersistentConfig) *LocalCachedDisk {
	return &LocalCachedDisk{
		logger:            logger,
		mu:                sync.Mutex{},
		fsync:             config.Strategy.Fsync,
		disk:              disk,
		cachedHardState:   emptyState,
		cachedEntries:     make([]raftpb.Entry, 0, config.Strategy.MaxLocalCacheSize),
		maxLocalCacheSize: config.Strategy.MaxLocalCacheSize,
		cachePreserveTime: config.Strategy.CachePreserveTime,
		cachePreserve:     nil,
	}
}
