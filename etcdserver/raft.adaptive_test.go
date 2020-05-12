package etcdserver

import (
	"fmt"
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

func (m *mockingIO) Clear() {
	m.RecorderBuffered = &testutil.RecorderBuffered{}
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

func (tsk *testSaucrKit) testMode(expected SaucrMode, orElse string) (string, bool) {
	c := tsk.pMonitor.IsCritical()
	f := tsk.pManager.fsync
	m := tsk.srn.currentMode

	if c != expected.IsCritical() || f != expected.IsFsync() || m != expected {
		return fmt.Sprintf("mode test failed (expected=%v) %s [%v,%v,%v]", expected, orElse, c, f, m), false
	} else {
		return "", true
	}
}

func (tsk *testSaucrKit) testPersist(expected bool, orElse string) (string, bool) {
	if expected {
		if len(tsk.bareDisk.Action()) == 0 || tsk.pManager.UnPersisted() {
			return fmt.Sprintf("persist test failed (expect=persisted) %s", orElse), false
		}
	} else {
		if len(tsk.bareDisk.Action()) != 0 || !tsk.pManager.UnPersisted() {
			return fmt.Sprintf("persist test failed (expect=not_persisted) %s", orElse), false
		}
	}

	return "", true
}

func (tsk *testSaucrKit) clearRecords(disk, network bool) {
	if disk {
		tsk.bareDisk.(*mockingIO).Clear()
	}

	if network {
		tsk.bareNetwork.(*mockingIO).Clear()
	}
}

func (tsk *testSaucrKit) waitForNotify() {
	<-(<-tsk.srn.applyc).notifyc
}

func TestSaucrRaftNodeHeartbeatAndSelfAwareness(t *testing.T) {

}

func TestSaucrLeaderStepOne(t *testing.T) {
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
		<-leader.srn.applyc

		if s, ok := leader.testMode(NORMAL, "init hb"); !ok {
			t.Error(s)
		}
		if s, ok := leader.testPersist(false, "init hb"); !ok {
			t.Error(s)
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
			if s, ok := leader.testMode(NORMAL, "recv hb"); !ok {
				t.Error(s)
			}
			if s, ok := leader.testPersist(false, "recv hb"); !ok {
				t.Error(s)
			}
		}
	}

	tests["ULeader -> NLeader"] = func(t *testing.T) {
		sLeaders, sFollowers, _ := GenerateDebugSaucrServers(
			1, 2, 0,
			GetQAlloc("monopoly"),
			GetMAlloc("uniform", SHELTERING),
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

		//ap := <-leader.srn.applyc
		<-leader.srn.applyc

		if s, ok := leader.testPersist(true, "init"); !ok {
			t.Error(s)
		}

		// <-ap.notifyc

		// receiving hb responses

		hbrp := make([]raftpb.Message, len(hb))

		for i := 0; i < 3; i++ {
			for i := 0; i < len(hb); i++ {
				hbrp[i] = raftpb.Message{
					Type: raftpb.MsgHeartbeatResp,
					To:   hb[i].From,
					From: hb[i].To,
				}

				leader.srv.Process(nil, hbrp[i])

				if s, ok := leader.testMode(SHELTERING, "recv hb"); !ok {
					t.Error(s)
				}
			}
			leader.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 5), Messages: hb}
			<-leader.srn.applyc
		}

		term := rand.Uint64()

		for i := 0; i < len(hb); i++ {
			hbrp[i] = raftpb.Message{
				Type: raftpb.MsgHeartbeatResp,
				To:   hb[i].From,
				From: hb[i].To,
				Term: term,
			}

			leader.srv.Process(nil, hbrp[i])
		}

		leader.clearRecords(false, true)

		for i, follower := range followers {
			hb[i] = raftpb.Message{
				Type: raftpb.MsgHeartbeat,
				To:   follower.id,
				From: leader.id,
				Term: term,
			}
		}

		leader.raft.readyc <- raft.Ready{HardState: raftpb.HardState{Term: term}, Entries: make([]raftpb.Entry, 5), Messages: hb}
		ap := <-leader.srn.applyc
		<-ap.notifyc

		time.After(300 * time.Millisecond)

		if s, ok := leader.testMode(NORMAL, "send hb again"); !ok {
			t.Error(s)
		}

		actions := leader.bareNetwork.Action()

		if len(actions) != len(hb)+len(followers) {
			t.Error("leader should broadcast its mode switch")
		}

		var count int

		for _, action := range actions {
			if action.Name == "Send" {
				msg := action.Params[0].(raftpb.Message)
				if msg.Term != term {
					t.Error("term is not update")
				}
				if msg.Type == raftpb.MsgSaucrNormal {
					count++
				}
			}
		}

		if count != len(followers) {
			t.Error("leader should broadcast message MsgSaucrNormal")
		}
	}

	tests["NLeader -> ULeader"] = func(t *testing.T) {
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

		for i := 0; i < 2; i++ {
			leader.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 5), Messages: hb}
			<-leader.srn.applyc
		}

		if s, ok := leader.testMode(NORMAL, "before transformation"); !ok {
			t.Error(s)
		}

		if s, ok := leader.testPersist(false, "before transformation"); !ok {
			t.Error(s)
		}

		term := rand.Uint64()

		for i, follower := range followers {
			hb[i] = raftpb.Message{
				Type: raftpb.MsgHeartbeat,
				To:   follower.id,
				From: leader.id,
				Term: term,
			}
		}

		leader.clearRecords(false, true)

		leader.raft.readyc <- raft.Ready{HardState: raftpb.HardState{Term: term}, Entries: make([]raftpb.Entry, 5), Messages: hb}
		ap := <-leader.srn.applyc
		<-ap.notifyc

		time.After(300 * time.Millisecond)

		if s, ok := leader.testMode(SHELTERING, "after transformation"); !ok {
			t.Error(s)
		}

		if s, ok := leader.testPersist(true, "after transformation"); !ok {
			t.Error(s)
		}

		actions := leader.bareNetwork.Action()

		if len(actions) != len(hb)+len(followers) {
			t.Error("leader should broadcast the mode change")
		}

		var count int

		for _, action := range actions {
			if action.Name == "Send" {
				msg := action.Params[0].(raftpb.Message)
				if msg.Term != term {
					t.Error("term is not update")
				}
				if msg.Type == raftpb.MsgSaucrSheltering {
					count++
				}
			}
		}

		if count != len(followers) {
			t.Error("leader should broadcast message MsgSaucrSheltering")
		}
	}

	tests["ULeader -> ULeader"] = func(t *testing.T) {
		sLeaders, sFollowers, _ := GenerateDebugSaucrServers(
			1, 2, 0,
			GetQAlloc("monopoly"),
			GetMAlloc("uniform", SHELTERING),
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
		<-leader.srn.applyc

		if s, ok := leader.testMode(SHELTERING, "init hb"); !ok {
			t.Error(s)
		}
		if s, ok := leader.testPersist(true, "init hb"); !ok {
			t.Error(s)
		}
	}

	tests["Leader -> Follower"] = func(t *testing.T) {
		sl, _, _ := GenerateDebugSaucrServers(
			2, 0, 3,
			GetQAlloc("leaderless"),
			GetMAlloc("bi-partitioned"),
		)

		nl := newTestSaucrKit(sl[0])
		nl.init()

		ul := newTestSaucrKit(sl[1])
		ul.init()

		defer func() {
			nl.srn.Stop()
			ul.srn.Stop()
		}()

		l := nl.srn.peers[2]

		nl.raft.readyc <- raft.Ready{SoftState: &raft.SoftState{Lead: l, RaftState: raft.StateFollower}}
		ul.raft.readyc <- raft.Ready{SoftState: &raft.SoftState{Lead: l, RaftState: raft.StateFollower}}

		<-nl.srn.applyc
		<-ul.srn.applyc

		if nl.pMonitor.GetConfig().Leader != l || ul.pMonitor.GetConfig().Leader != l {
			t.Error("leader has not update")
		}

		if nl.pMonitor.GetConfig().State != raft.StateFollower || ul.pMonitor.GetConfig().State != raft.StateFollower {
			t.Error("role has not update")
		}

		if s, ok := nl.testMode(NORMAL, ""); !ok {
			t.Error(s)
		}

		if s, ok := ul.testMode(SHELTERING, ""); !ok {
			t.Error(s)
		}
	}

	for k, v := range tests {
		t.Run(k, v)
	}
}

func TestSaucrFollowerStepOne(t *testing.T) {
	var tests = make(map[string]func(t *testing.T))

	tests["NFollower -> NFollower"] = func(t *testing.T) {
		sLeaders, sFollowers, _ := GenerateDebugSaucrServers(
			2, 3, 0,
			GetQAlloc("monopoly", 0),
			GetMAlloc("uniform", NORMAL),
		)

		leaders := make([]*testSaucrKit, len(sLeaders))
		for i := 0; i < len(sLeaders); i++ {
			leaders[i] = newTestSaucrKit(sLeaders[i])
			leaders[i].init()
		}

		followers := make([]*testSaucrKit, len(sFollowers))
		for i := 0; i < len(sFollowers); i++ {
			followers[i] = newTestSaucrKit(sFollowers[i])
			followers[i].init()
		}

		defer func() {
			for _, leader := range leaders {
				leader.srn.Stop()
			}
			for _, follower := range followers {
				follower.srn.Stop()
			}
		}()

		newTerm := uint64(0xcc)

		errChan := make([]chan string, len(followers))

		for i, follower := range followers {
			errChan[i] = make(chan string)
			go func(f *testSaucrKit, errC chan string) {
				defer close(errC)

				f.srv.Process(nil, raftpb.Message{Type: raftpb.MsgSaucrNormal, From: leaders[0].id, To: f.id})
				f.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 5)}
				<-(<-f.srn.applyc).notifyc

				if s, ok := f.testMode(NORMAL, "recv MsgNormal"); !ok {
					errC <- s
					return
				}
				if s, ok := f.testPersist(false, "recv MsgNormal"); !ok {
					errC <- s
					return
				}

				f.srv.Process(nil, raftpb.Message{Type: raftpb.MsgSaucrSheltering, From: leaders[0].id, To: f.id})
				f.raft.readyc <- raft.Ready{SoftState: &raft.SoftState{Lead: leaders[1].id}, HardState: raftpb.HardState{Term: newTerm}, Entries: make([]raftpb.Entry, 5)}
				f.waitForNotify()

				if s, ok := f.testMode(NORMAL, "recv expired MsgSheltering"); !ok {
					errC <- s
					return
				}
				if s, ok := f.testPersist(false, "recv expired MsgSheltering"); !ok {
					errC <- s
					return
				}

				f.srv.Process(nil, raftpb.Message{Type: raftpb.MsgSaucrSheltering, From: leaders[0].id, To: f.id})
				f.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 5)}
				f.waitForNotify()

				if s, ok := f.testMode(NORMAL, "recv expired MsgSheltering"); !ok {
					errC <- s
					return
				}
				if s, ok := f.testPersist(false, "recv expired MsgSheltering"); !ok {
					errC <- s
					return
				}
			}(follower, errChan[i])
		}

		for i, e := range errChan {
			for err := range e {
				t.Error("from [", i, "]: ", err)
			}
		}
	}

	tests["UFollower -> NFollower"] = func(t *testing.T) {
		sLeaders, sFollowers, _ := GenerateDebugSaucrServers(
			1, 2, 0,
			GetQAlloc("monopoly", 0),
			GetMAlloc("uniform", SHELTERING),
		)

		leaders := make([]*testSaucrKit, len(sLeaders))
		for i := 0; i < len(sLeaders); i++ {
			leaders[i] = newTestSaucrKit(sLeaders[i])
			leaders[i].init()
		}

		followers := make([]*testSaucrKit, len(sFollowers))
		for i := 0; i < len(sFollowers); i++ {
			followers[i] = newTestSaucrKit(sFollowers[i])
			followers[i].init()
		}

		defer func() {
			for _, leader := range leaders {
				leader.srn.Stop()
			}
			for _, follower := range followers {
				follower.srn.Stop()
			}
		}()

		errChan := make([]chan string, len(followers))

		for i, follower := range followers {
			errChan[i] = make(chan string)
			go func(f *testSaucrKit, errC chan string) {
				defer close(errC)
				f.clearRecords(true, false)
				f.srv.Process(nil, raftpb.Message{Type: raftpb.MsgSaucrNormal, From: leaders[0].id, To: f.id})
				f.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 5)}
				f.waitForNotify()

				if s, ok := f.testMode(NORMAL, "recv MsgNormal"); !ok {
					errC <- s
					return
				}
				if s, ok := f.testPersist(false, "recv MsgNormal"); !ok {
					errC <- s
					return
				}
			}(follower, errChan[i])
		}

		for i, e := range errChan {
			for err := range e {
				t.Error("from [", i, "]: ", err)
			}
		}
	}

	tests["NFollower -> UFollower"] = func(t *testing.T) {
		sLeaders, sFollowers, _ := GenerateDebugSaucrServers(
			1, 2, 0,
			GetQAlloc("monopoly", 0),
			GetMAlloc("uniform", NORMAL),
		)

		leaders := make([]*testSaucrKit, len(sLeaders))
		for i := 0; i < len(sLeaders); i++ {
			leaders[i] = newTestSaucrKit(sLeaders[i])
			leaders[i].init()
		}

		followers := make([]*testSaucrKit, len(sFollowers))
		for i := 0; i < len(sFollowers); i++ {
			followers[i] = newTestSaucrKit(sFollowers[i])
			followers[i].init()
		}

		defer func() {
			for _, leader := range leaders {
				leader.srn.Stop()
			}
			for _, follower := range followers {
				follower.srn.Stop()
			}
		}()

		errChan := make([]chan string, len(followers))

		for i, follower := range followers {
			errChan[i] = make(chan string)
			go func(f *testSaucrKit, errC chan string) {
				defer close(errC)

				f.clearRecords(true, false)
				f.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 6)}
				ap := <-f.srn.applyc
				<-ap.notifyc

				if s, ok := f.testPersist(false, "pre-requests"); !ok {
					errC <- s
					return
				}

				f.srv.Process(nil, raftpb.Message{Type: raftpb.MsgSaucrSheltering, From: leaders[0].id, To: f.id})
				f.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 4)}
				ap = <-f.srn.applyc
				<-ap.notifyc

				if s, ok := f.testMode(SHELTERING, "recv MsgSaucrSheltering"); !ok {
					errC <- s
					return
				}
				if s, ok := f.testPersist(true, "recv MsgSaucrSheltering"); !ok {
					errC <- s
					return
				}

				var count int
				for _, action := range f.bareDisk.Action() {
					if action.Name == "Save" {
						count += action.Params[1].(int)
					}
				}

				if count != 10 {
					errC <- "not all entries have been persisted"
					return
				}
			}(follower, errChan[i])
		}

		for i, e := range errChan {
			for err := range e {
				t.Error("from [", i, "]: ", err)
			}
		}
	}

	tests["UFollower -> UFollower"] = func(t *testing.T) {
		sLeaders, sFollowers, _ := GenerateDebugSaucrServers(
			2, 3, 0,
			GetQAlloc("leaderless"),
			GetMAlloc("uniform", SHELTERING),
		)

		leaders := make([]*testSaucrKit, len(sLeaders))
		for i := 0; i < len(sLeaders); i++ {
			leaders[i] = newTestSaucrKit(sLeaders[i])
			leaders[i].init()
		}

		followers := make([]*testSaucrKit, len(sFollowers))
		for i := 0; i < len(sFollowers); i++ {
			followers[i] = newTestSaucrKit(sFollowers[i])
			followers[i].init()
		}

		defer func() {
			for _, leader := range leaders {
				leader.srn.Stop()
			}
			for _, follower := range followers {
				follower.srn.Stop()
			}
		}()

		newTerm := uint64(0xcc)

		errChan := make([]chan string, len(followers))

		for i, follower := range followers {
			errChan[i] = make(chan string)
			go func(f *testSaucrKit, errC chan string) {
				defer close(errC)

				f.clearRecords(true, false)

				f.srv.Process(nil, raftpb.Message{Type: raftpb.MsgSaucrNormal, From: leaders[0].id, To: f.id, Term: newTerm})
				f.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 5)}
				f.waitForNotify()

				if s, ok := f.testMode(SHELTERING, "recv MsgNormal too early"); !ok {
					errC <- s
					return
				}
				if s, ok := f.testPersist(true, "recv MsgNormal too early"); !ok {
					errC <- s
					return
				}

				f.clearRecords(true, false)

				f.srv.Process(nil, raftpb.Message{Type: raftpb.MsgSaucrSheltering, From: leaders[0].id, To: f.id, Term: newTerm})
				f.raft.readyc <- raft.Ready{SoftState: &raft.SoftState{Lead: leaders[0].id}, HardState: raftpb.HardState{Term: newTerm}, Entries: make([]raftpb.Entry, 5)}
				f.waitForNotify()

				if s, ok := f.testMode(SHELTERING, "recv MsgSheltering"); !ok {
					errC <- s
					return
				}
				if s, ok := f.testPersist(true, "recv MsgSheltering"); !ok {
					errC <- s
					return
				}
			}(follower, errChan[i])
		}

		for i, e := range errChan {
			for err := range e {
				t.Error("from [", i, "]: ", err)
			}
		}
	}

	tests["Follower -> Candidate"] = func(t *testing.T) {
		_, sFollower, _ := GenerateDebugSaucrServers(1, 2, 0, GetQAlloc("table", []int{0, -1}), GetMAlloc("bi-partitioned"))

		followers := make([]*testSaucrKit, len(sFollower))

		for i, s := range sFollower {
			followers[i] = newTestSaucrKit(s)
			followers[i].init()
		}

		defer func() {
			for _, follower := range followers {
				follower.srn.Stop()
			}
		}()

		nf := followers[0]

		nf.raft.readyc <- raft.Ready{Entries: make([]raftpb.Entry, 4)}
		nf.waitForNotify()

		if s, ok := nf.testPersist(false, "pre-request"); !ok {
			t.Error(s)
		}

		for _, follower := range followers {
			follower.raft.readyc <- raft.Ready{
				SoftState: &raft.SoftState{Lead: raft.None, RaftState: raft.StateCandidate},
				HardState: raftpb.HardState{Term: rand.Uint64(), Vote: follower.id},
				Entries:   make([]raftpb.Entry, 6),
			}

			follower.waitForNotify()

			if follower.pMonitor.GetConfig().State != raft.StateCandidate {
				t.Error("role change failed")
			}

			if s, ok := follower.testMode(SHELTERING, "after change"); !ok {
				t.Error(s)
			}

			if s, ok := follower.testPersist(true, "after change"); !ok {
				t.Error(s)
			}
		}

		var count int
		for _, action := range nf.bareDisk.Action() {
			if action.Name == "Save" {
				count += action.Params[1].(int)
			}
		}

		if count != 10 {
			t.Error("not all entries have been persisted")
		}
	}

	for k, v := range tests {
		t.Run(k, v)
	}
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
	case "bi-partitioned":
		return func(i int, stateType raft.StateType) SaucrMode {
			if stateType == raft.StateLeader || stateType == raft.StateFollower {
				if i%2 == 0 {
					return NORMAL
				} else {
					return SHELTERING
				}
			} else {
				return SHELTERING
			}
		}
	default:
		return nil
	}
}
