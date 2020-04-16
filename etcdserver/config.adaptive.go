package etcdserver

type SaucrMode uint8

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
