package draft

import "go.etcd.io/etcd/raft/raftpb"

func ReorgEntries(term uint64, ent []raftpb.Entry) ([]raftpb.Entry, bool) {
	return nil, false
}

func RecordTermRange(r map[uint64][2]int, ent []raftpb.Entry) {

}
