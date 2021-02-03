package draft

import "go.etcd.io/etcd/raft/raftpb"

func ReorgEntries(term uint64, ent []raftpb.Entry) ([]raftpb.Entry, bool) {
	return nil, false
}
