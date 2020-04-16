package etcdserver

import (
	"go.etcd.io/etcd/pkg/mock/mockstorage"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"sync"
	"testing"
	"time"
)

func TestNewSaucrRaftNode(t *testing.T) {
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
