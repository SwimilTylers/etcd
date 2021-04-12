package draft

import (
	"fmt"
	"go.etcd.io/etcd/raft/raftpb"
	"path/filepath"
	"strconv"
)

func ft2rf(from, to uint64) (rack, file string) {
	file = "F" + strconv.Itoa(int(from))
	rack = "R" + strconv.Itoa(int(to))

	return rack, file
}

func rf2t(rack, file string) (token string) {
	token = filepath.Join(rack, file)
	return token
}

func generateRackFileNames(size int) (string, []string, string, []string) {
	lRack := "R0"
	rRacks := make([]string, size-1)
	for i := 0; i < size-1; i++ {
		rRacks[i] = "R" + strconv.Itoa(i+1)
	}

	lFile := "F0"
	rFiles := make([]string, size-1)
	for i := 0; i < size-1; i++ {
		rFiles[i] = "F" + strconv.Itoa(i+1)
	}

	return lRack, rRacks, lFile, rFiles
}

func offerWriterGrant(r string, f string, wg func(key string) IMFWriter) map[string]IMFWriter {
	res := make(map[string]IMFWriter)
	sig := filepath.Join(r, f)
	res[sig] = wg(sig)

	return res
}

func offerReaderGrant(r string, fs []string, rg func(key string) IMFReader) map[string]*updater {
	res := make(map[string]*updater)
	for _, f := range fs {
		sig := filepath.Join(r, f)
		res[sig] = &updater{
			next:   0,
			reader: rg(sig),
		}
	}

	return res
}

func offerCollectors(r string, fs []string, cg func(key string) Collector) map[string]Collector {
	res := make(map[string]Collector)
	for _, f := range fs {
		sig := filepath.Join(r, f)
		res[sig] = cg(sig)
	}

	return res
}

func era2ea(ent []*raftpb.Entry) []raftpb.Entry {
	res := make([]raftpb.Entry, len(ent))
	for i, entry := range ent {
		res[i] = *entry
	}
	return res
}

func ea2era(ent []raftpb.Entry) []*raftpb.Entry {
	res := make([]*raftpb.Entry, len(ent))
	for i, entry := range ent {
		res[i] = &entry
	}
	return res
}

func generateEntries(prevLogTerm, prevLogIndex uint64, desc []uint64) (uint64, uint64, []raftpb.Entry) {
	if len(desc) == 0 {
		return prevLogTerm, prevLogIndex, nil
	}

	res := make([]raftpb.Entry, len(desc))
	for i, t := range desc {
		res[i] = raftpb.Entry{
			Term:  t,
			Index: prevLogIndex + uint64(i+1),
			Type:  raftpb.EntryNormal,
			Data:  []byte(fmt.Sprintf("GE[%v]", t)),
		}
	}

	return prevLogTerm, prevLogIndex, res
}

var defaultCollectorGenerator = func(key string) Collector {
	return NewEntryFragmentCollector(true)
}
