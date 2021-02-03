package draft

import "go.etcd.io/etcd/raft/raftpb"

type Update struct {
	SourceFile string
	Term       uint64

	ZeroDelta bool

	Entries []raftpb.Entry
	VoteMsg *raftpb.Message
	Err     error
}

type SyncPrimitives interface {
	//Write persists draft message to the specific destination
	Write(rack, file string, message *raftpb.Message) error

	//GetUpdate fetches and categorizes the delta of the specific destination
	GetUpdate(rack, file string) *Update
}

type AsyncPrimitives interface {
	//AsyncWrite submits Write job and sends a signal to c when the job is done.
	AsyncWrite(rack, file string, c chan<- bool) error

	//AsyncGetUpdate submits GetUpdate job and sends asignal to c when the job is done.
	AsyncGetUpdate(rack, file string, c chan<- *Update) error
}

type Primitives interface {
	SyncPrimitives
	AsyncPrimitives
}
