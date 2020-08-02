package etcdserver

import (
	"go.etcd.io/etcd/adaptive"
	"strings"
	"time"
)

type SaucrConfig struct {
	MaxLocalCacheSize int
	CachePreserveTime time.Duration

	InitMode           SaucrMode
	UseDisabledMonitor bool
	HbcounterType      func() adaptive.HeartbeatCounter

	SaucrModeSync bool
	SaucrModeItv  time.Duration

	DamperWindowSize int
	DamperFluctuate  int
}

var DefaultSaucrConfig = &SaucrConfig{
	MaxLocalCacheSize:  adaptive.DefaultStrategy.MaxLocalCacheSize,
	CachePreserveTime:  adaptive.DefaultStrategy.CachePreserveTime,
	InitMode:           SHELTERING,
	UseDisabledMonitor: false,
	HbcounterType:      adaptive.PolarizedCautiousHbCounterFactory,
	SaucrModeSync:      true,
	SaucrModeItv:       100 * time.Millisecond,
}

type SaucrMode uint8

func (m SaucrMode) String() string {
	if m == NORMAL {
		return "NORMAL"
	} else if m == SHELTERING {
		return "SHELTERING"
	} else {
		return "UNKNOWN"
	}
}

func GetModeFromString(s string) SaucrMode {
	if strings.ToUpper(s) == "NORMAL" {
		return NORMAL
	} else {
		return SHELTERING
	}
}

const (
	// NORMAL MODE means that current peer network works fine.
	//
	// Therefore, SaucrRaftNode should prefer performance and reduce
	// the frequency of persistent operations.
	NORMAL SaucrMode = iota

	// SHELTERING MODE means that current peer network is at stake.
	//
	// Therefore, SaucrRaftNode should prefer reliability and insists to
	// persist.
	SHELTERING
)

func (m SaucrMode) IsCritical() bool {
	return m == SHELTERING
}

func (m SaucrMode) IsFsync() bool {
	return m == SHELTERING
}

func (m SaucrMode) IsConflictFromCritical(critical bool) bool {
	if critical {
		return m != SHELTERING
	} else {
		return m != NORMAL
	}
}

func (m SaucrMode) IsConflictFromFsync(fsync bool) bool {
	if fsync {
		return m != SHELTERING
	} else {
		return m != NORMAL
	}
}

func GetModeFromCritical(critical bool) SaucrMode {
	if critical {
		return SHELTERING
	} else {
		return NORMAL
	}
}

func GetModeFromFsync(fsync bool) SaucrMode {
	if fsync {
		return SHELTERING
	} else {
		return NORMAL
	}
}
