package etcdserver

import (
	"errors"
	"go.etcd.io/etcd/adaptive"
	"go.etcd.io/etcd/raft"
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

	// reminder of cache persistence
	cachePreserveReminder <-chan time.Time
}

func (lcd *LocalCachedDisk) UnPersisted() bool {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	return !raft.IsEmptyHardState(lcd.cachedHardState) || len(lcd.cachedEntries) != 0
}

func (lcd *LocalCachedDisk) Flush() error {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	return lcd.flushInternal("Flush", emptyState, nil)
}

func (lcd *LocalCachedDisk) flushInternal(callerName string, otherSourceHS raftpb.HardState, otherSourceEnt []raftpb.Entry) error {
	var hs = lcd.cachedHardState
	var entries = lcd.cachedEntries

	// check whether there is a otherSource caches
	if !raft.IsEmptyHardState(otherSourceHS) {
		hs = otherSourceHS
	}

	if otherSourceEnt != nil {
		entries = otherSourceEnt
	}

	// check if a dummy flush
	if len(entries) == 0 {
		err := lcd.disk.Save(hs, make([]raftpb.Entry, 0))
		if lcd.logger != nil {
			if err != nil {
				lcd.logger.Error("error occurs when persist cached HardState",
					zap.String("op", callerName),
					zap.Error(err),
				)
			} else {
				lcd.logger.Info("cached HardState persisted", zap.String("op", callerName))
			}
		}
		return err
	}

	err := lcd.disk.Save(hs, entries)

	// clear cache
	lcd.cachePreserveReminder = nil
	lcd.cachedEntries = lcd.cachedEntries[:0]
	lcd.cachedHardState = emptyState

	if lcd.logger != nil {
		if err != nil {
			lcd.logger.Error("error occurs when persist cached Entries and HardState",
				zap.String("op", callerName),
				zap.Error(err),
			)
		} else {
			lcd.logger.Info("cached Entries and HardState persisted", zap.String("op", callerName))
		}
	}

	return err
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

		lcd.updateStrategy(cfg.Strategy)
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

	lcd.updateStrategy(strategy)

	if strategy.Fsync && len(lcd.cachedEntries) > 0 {
		// if there is non-persisted entries, flush it immediately
		return lcd.flushInternal("SetStrategy", emptyState, nil)
	} else {
		return nil
	}
}

func (lcd *LocalCachedDisk) updateStrategy(strategy *adaptive.PersistentStrategy) {
	lcd.fsync = strategy.Fsync
	lcd.maxLocalCacheSize = strategy.MaxLocalCacheSize
	lcd.cachePreserveTime = strategy.CachePreserveTime

	if lcd.maxLocalCacheSize > cap(lcd.cachedEntries) {
		newCache := make([]raftpb.Entry, len(lcd.cachedEntries), lcd.maxLocalCacheSize+1)
		copy(newCache, lcd.cachedEntries)
		lcd.cachedEntries = newCache
	}

	if lcd.logger != nil {
		lcd.logger.Info("apply new strategy",
			zap.Bool("fsync", lcd.fsync),
			zap.Int("maxLocalCacheSize", lcd.maxLocalCacheSize),
			zap.String("cachePreserveTime", lcd.cachePreserveTime.String()),
		)
	}
}

func (lcd *LocalCachedDisk) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	curLen := len(lcd.cachedEntries)
	newLen := len(ents)

	select {
	case <-lcd.cachePreserveReminder:
		send := make([]raftpb.Entry, curLen+newLen)
		copy(send, lcd.cachedEntries)
		copy(send[curLen:], ents)

		return lcd.flushInternal("Save", st, send)
	default:
		if !lcd.fsync && curLen+newLen < lcd.maxLocalCacheSize {
			lcd.cachedEntries = lcd.cachedEntries[:curLen+newLen]
			copy(lcd.cachedEntries[curLen:], ents)

			if !raft.IsEmptyHardState(st) {
				lcd.cachedHardState = st
			}

			if lcd.cachePreserveReminder == nil {
				lcd.cachePreserveReminder = time.After(lcd.cachePreserveTime)
			}

			return nil
		} else {
			send := make([]raftpb.Entry, curLen+newLen)
			copy(send, lcd.cachedEntries)
			copy(send[curLen:], ents)

			return lcd.flushInternal("Save", st, send)
		}
	}
}

func (lcd *LocalCachedDisk) SaveSnap(snap raftpb.Snapshot) error {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	if raft.IsEmptySnap(snap) {
		select {
		case <-lcd.cachePreserveReminder:
			return lcd.flushInternal("SaveSnap", emptyState, nil)
		default:
			return nil
		}
	} else {
		_ = lcd.flushInternal("SaveSnap", emptyState, nil)
		return lcd.disk.SaveSnap(snap)
	}
}

func (lcd *LocalCachedDisk) Close() error {
	lcd.mu.Lock()
	defer lcd.mu.Unlock()

	_ = lcd.flushInternal("Close", emptyState, nil)

	return lcd.disk.Close()
}

func (lcd *LocalCachedDisk) AddRemoteDisk(desc *adaptive.PersistentRemoteDescriptor) error {
	return errors.New("unsupported operation")
}

func (lcd *LocalCachedDisk) RemoveRemoteDisk(desc *adaptive.PersistentRemoteDescriptor) error {
	return errors.New("unsupported operation")
}

func NewLocalCachedDisk(logger *zap.Logger, disk Storage, config *adaptive.PersistentConfig) *LocalCachedDisk {
	ret := &LocalCachedDisk{
		logger:                logger,
		mu:                    sync.Mutex{},
		fsync:                 config.Strategy.Fsync,
		disk:                  disk,
		cachedHardState:       emptyState,
		cachedEntries:         make([]raftpb.Entry, 0, config.Strategy.MaxLocalCacheSize),
		maxLocalCacheSize:     config.Strategy.MaxLocalCacheSize,
		cachePreserveTime:     config.Strategy.CachePreserveTime,
		cachePreserveReminder: nil,
	}

	if ret.logger != nil {
		ret.logger.Info("apply strategy",
			zap.Bool("fsync", ret.fsync),
			zap.Int("maxLocalCacheSize", ret.maxLocalCacheSize),
			zap.String("cachePreserveTime", ret.cachePreserveTime.String()),
		)
	}

	return ret
}
