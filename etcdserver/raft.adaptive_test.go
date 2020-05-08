package etcdserver

import (
	"go.etcd.io/etcd/adaptive"
	"go.etcd.io/etcd/etcdserver/api/membership"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/mock/mockstorage"
	"go.etcd.io/etcd/pkg/testutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockingIO struct {
	*testutil.RecorderBuffered
}

func (m *mockingIO) Send(msgs []raftpb.Message) {
	for _, msg := range msgs {
		if msg.To != 0 {
			m.Record(testutil.Action{Name: "Send", Params: []interface{}{msg}})
		}
	}
}

func (m *mockingIO) Start() error                          { return nil }
func (m *mockingIO) Handler() http.Handler                 { return nil }
func (m *mockingIO) SendSnapshot(msg snap.Message)         { m.Record(testutil.Action{Name: "SendSnapshot"}) }
func (m *mockingIO) AddRemote(id types.ID, urls []string)  {}
func (m *mockingIO) AddPeer(id types.ID, urls []string)    {}
func (m *mockingIO) RemovePeer(id types.ID)                {}
func (m *mockingIO) RemoveAllPeers()                       {}
func (m *mockingIO) UpdatePeer(id types.ID, urls []string) {}
func (m *mockingIO) ActiveSince(id types.ID) time.Time     { return time.Time{} }
func (m *mockingIO) ActivePeers() int                      { return 0 }
func (m *mockingIO) Stop()                                 {}

func (m *mockingIO) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	m.Record(testutil.Action{Name: "Save", Params: []interface{}{st, len(ents)}})
	return nil
}

func (m *mockingIO) SaveSnap(snap raftpb.Snapshot) error {
	if !raft.IsEmptySnap(snap) {
		m.Record(testutil.Action{Name: "SaveSnap"})
	}
	return nil
}

func (m *mockingIO) Close() error { return nil }

func newMockingIO() *mockingIO { return &mockingIO{RecorderBuffered: &testutil.RecorderBuffered{}} }

type testSaucrKit struct {
	leader uint64

	id    uint64
	peers []uint64

	srv  *EtcdSaucrServer
	srn  *SaucrRaftNode
	raft *readyNode

	pMonitor    *adaptive.SaucrMonitor
	pManager    *LocalCachedDisk
	bareDisk    testutil.Recorder
	bareNetwork testutil.Recorder

	rh *raftReadyHandler
}

func newTestSaucrKit(srv *EtcdSaucrServer) *testSaucrKit {
	kit := &testSaucrKit{
		leader:      srv.srn.PeerMonitor.GetConfig().Leader,
		srv:         srv,
		srn:         srv.srn,
		raft:        srv.srn.Node.(*readyNode),
		pMonitor:    srv.srn.PeerMonitor.(*adaptive.SaucrMonitor),
		pManager:    srv.srn.PManager.(*LocalCachedDisk),
		bareDisk:    srv.srn.storage.(*mockingIO),
		bareNetwork: srv.srn.transport.(*mockingIO),
	}

	kit.id = srv.srn.self
	kit.peers = srv.srn.peers
	kit.rh = &raftReadyHandler{
		getLead:              func() uint64 { return atomic.LoadUint64(&kit.leader) },
		updateLead:           func(lead uint64) { atomic.StoreUint64(&kit.leader, lead) },
		updateLeadership:     func(bool) {},
		updateCommittedIndex: func(uint64) {},
	}

	return kit
}

func (tsk *testSaucrKit) init() {
	role := tsk.pMonitor.GetConfig().State

	tsk.raft.readyc <- raft.Ready{SoftState: &raft.SoftState{Lead: tsk.leader, RaftState: role}}
	tsk.srn.start(tsk.rh)
	<-tsk.srn.applyc
}

func TestSaucrRaftNodeHeartbeatAndSelfAwareness(t *testing.T) {
	var tests = make(map[string]func(t *testing.T))

	tests["NLeader -> NLeader"] = func(t *testing.T) {
		sLeaders, sFollowers, _ := GenerateDebugSaucrServers(
			1, 2, 0,
			GetQAlloc("monopoly"),
			GetMAlloc("uniform", NORMAL),
		)

		leader := newTestSaucrKit(sLeaders[0])
		leader.init()

		followers := make([]*testSaucrKit, len(sFollowers))
		for i := 0; i < len(sFollowers); i++ {
			followers[i] = newTestSaucrKit(sFollowers[i])
			followers[i].init()
		}

		defer func() {
			leader.srn.Stop()
			for _, follower := range followers {
				follower.srn.Stop()
			}
		}()

		// sending hbs

		hb := make([]raftpb.Message, len(followers))

		for i, follower := range followers {
			hb[i] = raftpb.Message{
				Type: raftpb.MsgHeartbeat,
				To:   follower.id,
				From: leader.id,
			}
		}

		leader.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 5), Messages: hb}

		if leader.pMonitor.IsCritical() {
			t.Error("leader should stay normal before sending a round of hbs")
		}

		//ap := <-leader.srn.applyc
		<-leader.srn.applyc

		if leader.pMonitor.IsCritical() || leader.pManager.fsync || leader.srn.currentMode == SHELTERING {
			t.Errorf("leader should stay normal after sending a round of hbs, even not yet receiving any response, [%v,%v,%v]",
				leader.pMonitor.IsCritical(), leader.pManager.fsync, leader.srn.currentMode)
		}

		if len(leader.bareDisk.Action()) != 0 {
			t.Error("leader has persisted entries onto disk")
		}

		if !leader.pManager.UnPersisted() {
			t.Error("cached entries lost")
		}

		// <-ap.notifyc

		// receiving hb responses

		hbrp := make([]raftpb.Message, len(hb))

		for i := 0; i < len(hb); i++ {
			hbrp[i] = raftpb.Message{
				Type: raftpb.MsgHeartbeatResp,
				To:   hb[i].From,
				From: hb[i].To,
			}

			leader.srv.Process(nil, hbrp[i])

			if leader.pMonitor.IsCritical() || leader.pManager.fsync || leader.srn.currentMode == SHELTERING {
				t.Errorf("leader should stay normal when receiving any response, [%v,%v,%v]",
					leader.pMonitor.IsCritical(), leader.pManager.fsync, leader.srn.currentMode)
			}
		}

		if len(leader.bareDisk.Action()) != 0 {
			t.Error("leader has persisted entries onto disk")
		}

		if !leader.pManager.UnPersisted() {
			t.Error("cached entries lost")
		}
	}

	tests["ULeader -> NLeader"] = func(t *testing.T) {

	}

	tests["NLeader -> ULeader"] = func(t *testing.T) {

	}

	tests["NFollower -> UFollower"] = func(t *testing.T) {

	}

	tests["UFollower -> NFollower"] = func(t *testing.T) {

	}

	for k, v := range tests {
		t.Run(k, v)
	}
}

func TestSaucrRaftNodeStateChange(t *testing.T) {

}

func TestSaucrRaftNodeModeSwitch(t *testing.T) {

}

func TestNewSaucrRaftNode(t *testing.T) {
	t.Skip()
	n := newNopReadyNode()

	r := newRaftNode(raftNodeConfig{
		lg:          zap.NewExample(),
		Node:        n,
		storage:     mockstorage.NewStorageRecorder(""),
		raftStorage: raft.NewMemoryStorage(),
		transport:   newNopTransporter(),
	})
	srv := &EtcdServer{lgMu: new(sync.RWMutex), lg: zap.NewExample(), r: *r}

	srv.r.start(&raftReadyHandler{
		getLead:          func() uint64 { return 0 },
		updateLead:       func(uint64) {},
		updateLeadership: func(bool) {},
	})
	defer srv.r.Stop()

	n.readyc <- raft.Ready{
		SoftState:        &raft.SoftState{RaftState: raft.StateFollower},
		CommittedEntries: []raftpb.Entry{{Type: raftpb.EntryConfChange}},
	}
	ap := <-srv.r.applyc

	continueC := make(chan struct{})
	go func() {
		n.readyc <- raft.Ready{}
		<-srv.r.applyc
		close(continueC)
	}()

	select {
	case <-continueC:
		t.Fatalf("unexpected execution: raft routine should block waiting for apply")
	case <-time.After(time.Second):
	}

	// finish apply, unblock raft routine
	<-ap.notifyc

	select {
	case <-continueC:
	case <-time.After(time.Second):
		t.Fatalf("unexpected blocking on execution")
	}
}

func GenerateDebugSaucrServers(lNum, fNum, cNum int, qAlloc func(fIdx int) (lIdx int),
	mAlloc func(idx int, role raft.StateType) SaucrMode) (leaders, followers, candidates []*EtcdSaucrServer) {

	var total = lNum + fNum + cNum

	peers := make([]uint64, total)
	members := make([]*membership.Member, total)

	already := make(map[uint64]struct{})
	already[raft.None] = struct{}{}
	for i := 0; i < total; i++ {
		peers[i] = rand.Uint64()
		for {
			if _, ok := already[peers[i]]; ok {
				peers[i] = rand.Uint64()
			} else {
				already[peers[i]] = struct{}{}
				break
			}
		}
	}

	for i, peer := range peers {
		members[i] = &membership.Member{ID: types.ID(peer)}
	}

	leaders = make([]*EtcdSaucrServer, lNum)

	for i := 0; i < lNum; i++ {
		mode := mAlloc(i, raft.StateLeader)
		cluster := newTestCluster(members)
		leaders[i] = &EtcdSaucrServer{}
		leaders[i].EtcdServer = &EtcdServer{
			r: *newRaftNode(raftNodeConfig{
				lg:          zap.NewExample(),
				Node:        newNopReadyNode(),
				raftStorage: raft.NewMemoryStorage(),
				storage:     newMockingIO(),
				transport:   newMockingIO(),
			}),
			lgMu:    new(sync.RWMutex),
			lg:      zap.NewExample(),
			cluster: cluster,
		}

		leaders[i].r.isIDRemoved = func(id uint64) bool {
			return cluster.IsIDRemoved(types.ID(id))
		}

		leaders[i].srn = NewSaucrRaftNode(
			&leaders[i].r,
			&adaptive.PerceptibleConfig{
				State:    raft.StateLeader,
				Leader:   peers[i],
				Self:     peers[i],
				Critical: mode.IsCritical(),
				Peers:    peers,
			}, &adaptive.PersistentStrategy{
				Fsync:             mode.IsFsync(),
				MaxLocalCacheSize: adaptive.DefaultStrategy.MaxLocalCacheSize,
				CachePreserveTime: adaptive.DefaultStrategy.CachePreserveTime,
			},
		)
	}

	followers = make([]*EtcdSaucrServer, fNum)

	for i := 0; i < fNum; i++ {
		mode := mAlloc(i, raft.StateFollower)
		cluster := newTestCluster(members)

		var leader uint64
		if qAlloc(i) < 0 {
			leader = raft.None
		} else if qAlloc(i) < lNum {
			leader = peers[qAlloc(i)]
		} else {
			leader = raft.None
		}

		followers[i] = &EtcdSaucrServer{}
		followers[i].EtcdServer = &EtcdServer{
			r: *newRaftNode(raftNodeConfig{
				lg:          zap.NewExample(),
				Node:        newNopReadyNode(),
				storage:     newMockingIO(),
				raftStorage: raft.NewMemoryStorage(),
				transport:   newMockingIO(),
			}),
			lgMu:    new(sync.RWMutex),
			lg:      zap.NewExample(),
			cluster: cluster,
		}

		followers[i].r.isIDRemoved = func(id uint64) bool {
			return cluster.IsIDRemoved(types.ID(id))
		}

		followers[i].srn = NewSaucrRaftNode(
			&followers[i].r,
			&adaptive.PerceptibleConfig{
				State:    raft.StateFollower,
				Leader:   leader,
				Self:     peers[i+lNum],
				Critical: mode.IsCritical(),
				Peers:    peers,
			}, &adaptive.PersistentStrategy{
				Fsync:             mode.IsFsync(),
				MaxLocalCacheSize: adaptive.DefaultStrategy.MaxLocalCacheSize,
				CachePreserveTime: adaptive.DefaultStrategy.CachePreserveTime,
			},
		)
	}

	candidates = make([]*EtcdSaucrServer, cNum)

	for i := 0; i < cNum; i++ {
		mode := mAlloc(i, raft.StateCandidate)
		cluster := newTestCluster(members)

		candidates[i] = &EtcdSaucrServer{}
		candidates[i].EtcdServer = &EtcdServer{
			r: *newRaftNode(raftNodeConfig{
				lg:          zap.NewExample(),
				Node:        newNopReadyNode(),
				storage:     newMockingIO(),
				raftStorage: raft.NewMemoryStorage(),
				transport:   newMockingIO(),
			}),
			lgMu:    new(sync.RWMutex),
			lg:      zap.NewExample(),
			cluster: cluster,
		}

		candidates[i].r.isIDRemoved = func(id uint64) bool {
			return cluster.IsIDRemoved(types.ID(id))
		}

		candidates[i].srn = NewSaucrRaftNode(
			&candidates[i].r,
			&adaptive.PerceptibleConfig{
				State:    raft.StateCandidate,
				Leader:   raft.None,
				Self:     peers[i+lNum+fNum],
				Critical: mode.IsCritical(),
				Peers:    peers,
			}, &adaptive.PersistentStrategy{
				Fsync:             mode.IsFsync(),
				MaxLocalCacheSize: adaptive.DefaultStrategy.MaxLocalCacheSize,
				CachePreserveTime: adaptive.DefaultStrategy.CachePreserveTime,
			},
		)
	}

	return leaders, followers, candidates
}

func GetQAlloc(qType string, params ...interface{}) func(int) int {
	switch strings.ToLower(qType) {
	case "leaderless":
		return func(i int) int { return -1 }
	case "monopoly":
		if len(params) >= 1 {
			if lid, ok := params[0].(int); ok {
				return func(i int) int { return lid }
			}
		}
		return func(i int) int { return 0 }
	case "table":
		var table = params[0].([]int)
		return func(i int) int { return table[i] }
	default:
		return nil
	}
}

func GetMAlloc(mType string, params ...interface{}) func(int, raft.StateType) SaucrMode {
	switch strings.ToLower(mType) {
	case "uniform":
		var mode SaucrMode
		switch params[0].(type) {
		case bool:
			mode = GetModeFromCritical(params[0].(bool))
		case SaucrMode:
			mode = params[0].(SaucrMode)
		case string:
			mode = GetModeFromString(params[0].(string))
		}

		return func(i int, stateType raft.StateType) SaucrMode {
			if stateType == raft.StateLeader || stateType == raft.StateFollower {
				return mode
			} else {
				return SHELTERING
			}
		}
	default:
		return nil
	}
}
