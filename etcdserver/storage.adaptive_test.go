package etcdserver

import (
	"go.etcd.io/etcd/adaptive"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const (
	saving uint8 = iota
	snapping
	closing
)

type LeakyStorageAction struct {
	issue uint8

	st   raftpb.HardState
	ents []raftpb.Entry

	snap raftpb.Snapshot
}

type LeakyStorage struct {
	mu      sync.Mutex
	devNull chan *LeakyStorageAction
	used    bool
}

func (ls *LeakyStorage) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.devNull <- &LeakyStorageAction{
		issue: saving,
		st:    st,
		ents:  ents,
	}

	ls.used = true

	return nil
}

func (ls *LeakyStorage) SaveSnap(snap raftpb.Snapshot) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.devNull <- &LeakyStorageAction{
		issue: snapping,
		snap:  snap,
	}

	ls.used = true

	return nil
}

func (ls *LeakyStorage) Close() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.devNull <- &LeakyStorageAction{
		issue: closing,
	}

	ls.used = true
	close(ls.devNull)

	return nil
}

func (ls *LeakyStorage) testAndReset() bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ret := ls.used
	ls.used = false

	return ret
}

func TestFsync(t *testing.T) {
	strategy, _, disk := GenerateStorage(true, 500, 50, true)

	dd := disk.disk.(*LeakyStorage)

	if err := disk.Save(GenerateEntries()); err != nil {
		t.Fatal("error occurs when saving")
	}

	if !dd.testAndReset() {
		t.Error("delayed persistence")
	}

	strategy.Fsync = false

	_ = disk.SetStrategy(strategy)

	if err := disk.Save(GenerateEntries()); err != nil {
		t.Fatal("error occurs when saving")
	}

	if dd.testAndReset() {
		t.Error("without cache")
	}

	// t.Log("Waiting 500ms before next save")
	<-time.After(600 * time.Millisecond)

	if err := disk.Save(GenerateEntries()); err != nil {
		t.Fatal("error occurs when saving")
	}

	if !dd.testAndReset() {
		t.Error("delayed persistence")
	}

	_ = disk.Flush()

	if !raft.IsEmptyHardState(disk.cachedHardState) ||
		len(disk.cachedEntries) > 0 || disk.cachePreserveReminder != nil {

		t.Error("dirty flush")
	}

	strategy.CachePreserveTime = time.Second
	_ = disk.SetStrategy(strategy)

	dd.testAndReset()

	st, ents := GenerateEntries()

	_ = disk.Save(st, ents)

	strategy.Fsync = true
	_ = disk.SetStrategy(strategy)

	if !dd.testAndReset() {
		t.Error("when fsync=true, flush should done immediately")
	}
}

func TestCache(t *testing.T) {
	strategy, _, disk := GenerateStorage(false, 500, 200, true)

	dd := disk.disk.(*LeakyStorage)

	finished := make(chan int)

	go func() {
		totalLen := 0
		for i := 0; i < 9; i++ {
			st, ents := GenerateEntries()
			totalLen += len(ents)
			_ = disk.Save(st, ents)
			if dd.testAndReset() {
				t.Error("without cache, @ entry", i)
			}
		}
		finished <- totalLen
	}()

	if dd.testAndReset() {
		t.Error("without cache")
	}

	length := <-finished

	if dd.testAndReset() {
		t.Error("without cache")
	}

	_ = disk.Flush()

	if !dd.testAndReset() {
		t.Error("delayed persistence")
	}

	if length != len((<-dd.devNull).ents) {
		t.Error("miss some entries")
	}

	dd.testAndReset()

	go func() {
		totalLen := 0
		for i := 0; i < 5; i++ {
			st, ents := GenerateEntries()
			totalLen += len(ents)
			_ = disk.Save(st, ents)
		}
		<-time.After(time.Duration(600) * time.Millisecond)
		finished <- totalLen
	}()

	length = <-finished

	st, ents := GenerateEntries()

	_ = disk.Save(st, ents)

	if !dd.testAndReset() {
		t.Error("delayed persistent")
	}

	if len((<-dd.devNull).ents) != length+len(ents) {
		t.Error("miss some entries")
	}

	_ = disk.Save(st, ents)

	strategy.Fsync = true
	_ = disk.SetStrategy(strategy)

	if !dd.testAndReset() {
		t.Error("when fsync=true, flush should done immediately")
	}

	if len((<-dd.devNull).ents) != len(ents) {
		t.Error("miss some entries")
	}

	go func() {
		totalLen := 0
		for i := 0; i < 200; i++ {
			st, ents := GenerateEntries()
			totalLen += len(ents)
			_ = disk.Save(st, ents)
		}
		finished <- totalLen
	}()

	length = <-finished

	if length < 200 {
		t.Skip("insufficient entries, len =", length)
	}

	if !dd.testAndReset() {
		t.Error("delayed persistence")
	}

	_ = disk.Flush()
	_ = disk.disk.Close()

	persistedLen := 0

	for issue := range dd.devNull {
		persistedLen += len(issue.ents)
	}

	if length != persistedLen {
		t.Error("miss some entries")
	}
}

func GenerateStorage(fsync bool, waitMillis int, cacheSize int, buffered bool) (*adaptive.PersistentStrategy, chan *LeakyStorageAction, *LocalCachedDisk) {
	var strat = &adaptive.PersistentStrategy{
		Fsync:             fsync,
		MaxLocalCacheSize: cacheSize,
		CachePreserveTime: time.Duration(waitMillis) * time.Millisecond,
	}

	var config = &adaptive.PersistentConfig{
		Strategy: strat,
		Remotes:  nil,
	}

	var saved chan *LeakyStorageAction

	if buffered {
		saved = make(chan *LeakyStorageAction, 1000)
	} else {
		saved = make(chan *LeakyStorageAction)
	}

	return strat, saved, NewLocalCachedDisk(zap.NewExample(), &LeakyStorage{sync.Mutex{}, saved, false}, config)
}

func GenerateEntries() (raftpb.HardState, []raftpb.Entry) {
	var state = raftpb.HardState{
		Term:   rand.Uint64(),
		Vote:   rand.Uint64(),
		Commit: rand.Uint64(),
	}

	var entries = make([]raftpb.Entry, rand.Intn(10)+1)

	for i := 0; i < len(entries); i++ {
		entries[i] = raftpb.Entry{
			Term:  state.Term,
			Index: uint64(i),
			Type:  raftpb.EntryNormal,
			Data:  nil,
		}
	}

	return state, entries
}
