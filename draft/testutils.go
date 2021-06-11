package draft

import (
	"fmt"
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
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

func offerCollectors(r string, fs []string, cg func(key string) collector.EntryFragmentCollector) map[string]collector.EntryFragmentCollector {
	res := make(map[string]collector.EntryFragmentCollector)
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
		index := prevLogIndex + uint64(i+1)
		res[i] = raftpb.Entry{
			Term:  t,
			Index: index,
			Type:  raftpb.EntryNormal,
			Data:  []byte(fmt.Sprintf("TESTUTILS.GE[term=%v,index=%v]", t, index)),
		}
	}

	return prevLogTerm, prevLogIndex, res
}

func generateEntryDesc(size int, rnd *rand.Rand) (uint64, uint64, []uint64) {
	plt := rnd.Uint64() & 0xffffffff
	pli := rnd.Uint64() & 0xffff

	term := plt + (rnd.Uint64() & 0xff)

	res := make([]uint64, size)
	for i := 0; i < size; i++ {
		res[i] = term
		term += rnd.Uint64() & 0xff
	}

	return plt, pli, res
}

func extendEntryDesc(extSize int, rnd *rand.Rand, logTerm, logIndex uint64, entries []uint64) (uint64, uint64, []uint64) {
	term := logTerm + (rnd.Uint64() & 0xff)
	bLen := len(entries)

	if bLen != 0 {
		term = entries[bLen-1] + (rnd.Uint64() & 0xff)
	}

	res := make([]uint64, bLen+extSize)
	copy(res, entries)

	for i := bLen; i < len(res); i++ {
		res[i] = term
		term += rnd.Uint64() & 0xff
	}

	return logTerm, logIndex, res
}

func forkEntryDesc(sharedSize, extChildSize int, rnd *rand.Rand, logTerm, logIndex uint64, entries []uint64) (sharedLogTerm, sharedLogIndex uint64, parent, child []uint64) {
	if sharedSize == 0 {
		_, _, child = generateEntryDesc(sharedSize+extChildSize, rnd)
		return logTerm, logIndex, entries, child
	}

	if extChildSize == 0 {
		return logTerm, logIndex, entries, append([]uint64{}, entries[:sharedSize]...)
	}

	_, _, child = extendEntryDesc(extChildSize, rnd, logTerm, logIndex, entries[:sharedSize])
	return logTerm, logIndex, entries, child
}

func messageToStrings(message []*raftpb.Message, desc ...[]uint64) string {
	builder := strings.Builder{}
	dIdx := 0
	for i, m := range message {
		switch m.Type {
		case raftpb.MsgVote, raftpb.MsgPreVote:
			builder.WriteString(fmt.Sprintf("[%02d]\t%s\t[term=%v]\n", i, m.Type.String(), m.Term))
		case raftpb.MsgApp:
			from := locateEntryDesc(m.Entries[0].Term, desc[dIdx])
			for from == -1 {
				dIdx++
				if dIdx == len(desc) {
					panic("desc incomplete")
				}
				from = locateEntryDesc(m.Entries[0].Term, desc[dIdx])
			}
			to := locateEntryDesc(m.Entries[len(m.Entries)-1].Term, desc[dIdx])
			for to == -1 {
				dIdx++
				if dIdx >= len(desc) {
					panic("desc incomplete")
				}
				to = locateEntryDesc(m.Entries[len(m.Entries)-1].Term, desc[dIdx])
			}
			builder.WriteString(fmt.Sprintf("[%02d]\t%s\t[term=%v,app=[%v,%v]]\n", i, m.Type.String(), m.Term, from, to))
		case raftpb.MsgHeartbeat:
			builder.WriteString(fmt.Sprintf("[%02d]\t%s\t[term=%v]\n", i, m.Type.String(), m.Term))
		default:
			builder.WriteString(fmt.Sprintf("[%02d]\t%s\n", i, m.Type.String()))
		}
	}

	return builder.String()
}

func locateEntryDesc(term uint64, desc []uint64) int {
	for i, u := range desc {
		if term == u {
			return i
		}
	}
	return -1
}
