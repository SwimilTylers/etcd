package draft

import "go.etcd.io/etcd/raft/raftpb"

type IMFReader interface {
	ReadIMF(readFromIdx int) ([]raftpb.Message, error)
}

type IMFWriter interface {
	WriteIMF(message *raftpb.Message) error
}
