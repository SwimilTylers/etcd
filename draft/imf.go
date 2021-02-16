package draft

import "go.etcd.io/etcd/raft/raftpb"

type IMFReader interface {
	ReadIMF(readFromIdx int) []raftpb.Message
}

type IMFWriter interface {
	WriteIMF(message *raftpb.Message) error
}
