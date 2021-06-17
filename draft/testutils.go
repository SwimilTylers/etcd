package draft

import (
	"fmt"
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
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

func cutEntryDesc(logTerm, logIndex uint64, entries []uint64, from, to int) (uint64, uint64, []uint64) {
	if from > to {
		panic("illegal argument")
	}

	if from == to {
		return logTerm, logIndex, nil
	}

	if from == 0 {
		return logTerm, logIndex, entries[:to]
	} else {
		return entries[from-1], logIndex + uint64(from), entries[from:to]
	}
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

type testOracle interface {
	GetOracle(name string) func(o ...interface{}) bool
}

type entryComparator struct {
	logTerm, logIndex uint64
	ent               []raftpb.Entry
	entLen            int
}

func newEntryComparator(logTerm uint64, logIndex uint64, ent []raftpb.Entry) *entryComparator {
	return &entryComparator{logTerm: logTerm, logIndex: logIndex, ent: ent, entLen: len(ent)}
}

func (ec *entryComparator) FetchEntriesWithStartIndex(startIndex uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if startIndex <= ec.logIndex+1 {
		return true, ec.ent, ec.logTerm, ec.logIndex
	}

	lastIndex := ec.ent[ec.entLen-1].Index
	if startIndex > lastIndex {
		return false, nil, 0, 0
	}

	startIndex = startIndex - ec.ent[0].Index
	logTerm, logIndex := ec.ent[startIndex-1].Term, ec.ent[startIndex-1].Index

	return true, ec.ent[startIndex:], logTerm, logIndex
}

func (ec *entryComparator) FetchAllEntries() (bool, []raftpb.Entry, uint64, uint64) {
	return true, ec.ent, ec.logTerm, ec.logIndex
}

//EquivEntrySeq checks if it shares the same entry sequence.
//
// This function accepts the following types of argument(s):
//	1. EquivEntrySeq(*collector.EntryFragment)
//	2. EquivEntrySeq(collector.EntryFragmentCollector)
//	3. EquivEntrySeq(collector.Locator)
//	4. EquivEntrySeq(collector.EntryFetcher)
//	5. EquivEntrySeq(uint64,uint64,[]raftpb.Entry)
func (ec *entryComparator) EquivEntrySeq(o ...interface{}) bool {
	if len(o) == 1 {
		switch o[0].(type) {
		case *collector.EntryFragment:
			f := o[0].(*collector.EntryFragment)
			return f.LogTerm == ec.logTerm && f.LogIndex == ec.logIndex && reflect.DeepEqual(f.Fragment, ec.ent)
		case collector.EntryFragmentCollector:
			c := o[0].(collector.EntryFragmentCollector)
			ok, fs := c.FetchAllFragments()
			if !ok || len(fs) != 1 {
				return false
			}
			return ec.EquivEntrySeq(fs[0])
		case collector.Locator:
			l := o[0].(collector.Locator)
			if l.IsEmpty() {
				return false
			}

			if l.MatchIndex(ec.logIndex, ec.logTerm) != collector.PREV {
				return false
			}

			if l.MatchIndex(ec.ent[ec.entLen-1].Index+1, 0) != collector.OVERFLOW {
				return false
			}

			for _, e := range ec.ent {
				if l.MatchIndex(e.Index, e.Term) != collector.WITHIN {
					return false
				}
			}

			return true
		case collector.EntryFetcher:
			f := o[0].(collector.EntryFetcher)
			ok, ent, lt, li := f.FetchAllEntries()
			if !ok {
				return false
			}
			return lt == ec.logTerm && li == ec.logIndex && reflect.DeepEqual(ent, ec.ent)
		}
	} else if len(o) == 3 {
		logTerm := o[0].(uint64)
		logIndex := o[1].(uint64)
		ent := o[2].([]raftpb.Entry)

		return logTerm == ec.logTerm && logIndex == ec.logIndex && reflect.DeepEqual(ent, ec.ent)
	}

	panic("illegal arguments")
}

func (ec *entryComparator) GetOracle(name string) func(o ...interface{}) bool {
	if name != "exact" {
		panic("illegal argument")
	}

	return ec.EquivEntrySeq
}

func (ec *entryComparator) String() string {
	terms := make([]uint64, len(ec.ent))
	for i, entry := range ec.ent {
		terms[i] = entry.Term
	}
	return fmt.Sprintf("logTerm=%v, logIndex=%v, ent=%v", ec.logTerm, ec.logIndex, terms)
}

type entryBranchForkOperator struct {
	logIndex, logTerm uint64
	entries           map[string][]uint64

	forkPoint  map[string]int
	forkParent map[string]string
	forkChild  map[string][]string

	major string
}

func newEntryBranchForkOperator() *entryBranchForkOperator {
	return &entryBranchForkOperator{
		entries:    map[string][]uint64{},
		forkPoint:  map[string]int{},
		forkParent: map[string]string{},
		forkChild:  map[string][]string{},
	}
}

func (bf *entryBranchForkOperator) AnchorMajorBranch(size int, rnd *rand.Rand) {
	bf.AnchorNamedMajorBranch("major", size, rnd)
}

func (bf *entryBranchForkOperator) AnchorNamedMajorBranch(name string, size int, rnd *rand.Rand) {
	bf.major = name
	bf.logTerm, bf.logIndex, bf.entries[name] = generateEntryDesc(size, rnd)
}

//ForkMajorNamedBranch fork a branch from the major branch with its name
func (bf *entryBranchForkOperator) ForkMajorNamedBranch(child string, forkPoint, extSize int, rnd *rand.Rand) {
	name := child

	_, _, _, bf.entries[name] = forkEntryDesc(forkPoint, extSize, rnd, bf.logTerm, bf.logIndex, bf.entries[bf.major])
	bf.forkPoint[name] = forkPoint
	bf.forkParent[name] = bf.major
	bf.forkChild[bf.major] = append(bf.forkChild[bf.major], name)
}

//ForkMinorNamedBranch fork a branch from a minor branch with its name
func (bf *entryBranchForkOperator) ForkMinorNamedBranch(child, parent string, relativeForkPoint, extSize int, rnd *rand.Rand) {
	if parent == bf.major {
		bf.ForkMajorNamedBranch(child, relativeForkPoint, extSize, rnd)
	}

	name := child
	forkPoint := relativeForkPoint + bf.forkPoint[parent]

	_, _, _, bf.entries[name] = forkEntryDesc(forkPoint, extSize, rnd, bf.logTerm, bf.logIndex, bf.entries[parent])
	bf.forkPoint[name] = forkPoint
	bf.forkParent[name] = parent
	bf.forkChild[parent] = append(bf.forkChild[parent], name)
}

//ForkMajorBranch fork a branch from the major branch
func (bf *entryBranchForkOperator) ForkMajorBranch(forkPoint, extSize int, rnd *rand.Rand) string {
	name := fmt.Sprintf("minor-%v", rnd.Uint64()&0xffffff)
	bf.ForkMajorNamedBranch(name, forkPoint, extSize, rnd)
	return name
}

//ForkMinorBranch fork a branch from a minor branch
func (bf *entryBranchForkOperator) ForkMinorBranch(parent string, relativeForkPoint, extSize int, rnd *rand.Rand) string {
	if parent == bf.major {
		return bf.ForkMajorBranch(relativeForkPoint, extSize, rnd)
	}

	name := fmt.Sprintf("%s-%v", parent, rnd.Uint64()&0xffffff)
	bf.ForkMinorNamedBranch(name, parent, relativeForkPoint, extSize, rnd)

	return name
}

func (bf *entryBranchForkOperator) Tracing(name string) ([]string, []int) {
	var branch []string
	var fork []int

	for name != bf.major {
		branch = append([]string{bf.forkParent[name]}, branch...)
		fork = append([]int{bf.forkPoint[name]}, fork...)
		name = bf.forkParent[name]
	}

	return branch, fork
}

func (bf *entryBranchForkOperator) GetMajorName() string {
	return bf.major
}

func (bf *entryBranchForkOperator) GetMajorTerm() uint64 {
	ent := bf.entries[bf.major]
	return ent[len(ent)-1]
}

func (bf *entryBranchForkOperator) GetMinorTerm(name string) uint64 {
	ent := bf.entries[name]
	return ent[len(ent)-1]
}

func (bf *entryBranchForkOperator) GetMajor() (uint64, uint64, []raftpb.Entry) {
	return generateEntries(bf.logTerm, bf.logIndex, bf.entries[bf.major])
}

func (bf *entryBranchForkOperator) GetMinor(name string, onlyForked bool) (uint64, uint64, []raftpb.Entry) {
	start := 0
	if onlyForked {
		start = bf.forkPoint[name]
	}
	ent := bf.entries[name]
	return generateEntries(cutEntryDesc(bf.logTerm, bf.logIndex, ent, start, len(ent)))
}

func (bf *entryBranchForkOperator) Oracle(branch string) *branchOracle {
	return &branchOracle{
		bf:       bf,
		shouldBe: branch,
	}
}

type branchOracle struct {
	bf       *entryBranchForkOperator
	shouldBe string
}

func (bo *branchOracle) GetOracle(name string) func(o ...interface{}) bool {
	switch name {
	case "exact":
		return newEntryComparator(bo.bf.GetMinor(bo.shouldBe, false)).GetOracle(name)
	case "inherit":
		return func(o ...interface{}) bool {
			l := o[0].(collector.Locator)
			lastIndex := l.LastIndex()
			_, lastTerm := l.LocateIndex(lastIndex)

			return isInherit(bo.bf, bo.shouldBe, extractBranchNameFromTermAndIndex(bo.bf, lastTerm, lastIndex))
		}
	default:
		panic("implement me")
	}
}

func deleteBranches(branches []string, deleted ...string) []string {
	var res []string
	dMap := make(map[string]struct{}, len(deleted))
	for _, d := range deleted {
		dMap[d] = struct{}{}
	}
	for _, branch := range branches {
		if _, ok := dMap[branch]; !ok {
			res = append(res, branch)
		}
	}
	return res
}

func gatherTraceFromBranches(bf *entryBranchForkOperator, branches ...string) string {
	sb := &strings.Builder{}
	for _, branch := range branches {
		trace, forkPoint := bf.Tracing(branch)
		for i := 0; i < len(trace); i++ {
			sb.WriteString(fmt.Sprintf("%s [%02d] ", trace[i], forkPoint[i]))
		}
		sb.WriteString(fmt.Sprintf("%s\n", branch))
	}
	return sb.String()
}

func extractBranchNameFromTermAndIndex(bf *entryBranchForkOperator, term, index uint64) string {
	var res string

	for name, _ := range bf.entries {
		_, _, ent := bf.GetMinor(name, true)
		bLast := ent[len(ent)-1]
		if bLast.Index == index && bLast.Term == term {
			return name
		}
	}

	return res
}

func extractBranchNameFromEntries(bf *entryBranchForkOperator, entries []raftpb.Entry) string {
	var res string

	if len(entries) == 0 {
		panic("cannot locate an empty branch")
	}

	last := entries[len(entries)-1]

	for name, _ := range bf.entries {
		_, _, ent := bf.GetMinor(name, true)
		bLast := ent[len(ent)-1]
		if reflect.DeepEqual(last, bLast) {
			return name
		}
	}

	return res
}

func isInherit(bf *entryBranchForkOperator, child, parent string) bool {
	asc, _ := bf.Tracing(child)
	for _, a := range asc {
		if parent == a {
			return true
		}
	}
	return false
}

func getMaxTermAmongBranches(bfs ...*entryBranchForkOperator) uint64 {
	var res uint64 = 0
	for _, bf := range bfs {
		if bf.logTerm > res {
			res = bf.logTerm
		}

		for _, entries := range bf.entries {
			if len(entries) > 0 {
				term := entries[len(entries)-1]
				if term > res {
					res = term
				}
			}
		}
	}

	return res
}

func assignUniqueTermAmongBranches(startWith uint64, bf *entryBranchForkOperator, rnd *rand.Rand) map[string]uint64 {
	entries := make([]string, len(bf.entries))
	top := 0
	for name, _ := range bf.entries {
		entries[top] = name
		top++
	}
	sort.Slice(entries, func(i, j int) bool {
		return -1 == strings.Compare(entries[i], entries[j])
	})

	res := make(map[string]uint64, len(entries))
	assigned := map[uint64]struct{}{}
	for _, name := range entries {
		term := startWith + rnd.Uint64()&0xffff
		_, ok := assigned[term]
		for ok {
			term = startWith + rnd.Uint64()&0xffff
			_, ok = assigned[term]
		}
		res[name] = term
		assigned[term] = struct{}{}
	}
	return res
}

func shuffleTMap(tMap map[string]uint64, rnd *rand.Rand) map[string]uint64 {
	names := make([]string, len(tMap))
	terms := make([]uint64, len(tMap))
	top := 0
	for n, t := range tMap {
		names[top] = n
		terms[top] = t
		top++
	}

	sort.Slice(names, func(i, j int) bool {
		return -1 == strings.Compare(names[i], names[j])
	})
	sort.Slice(terms, func(i, j int) bool {
		return terms[i] < terms[j]
	})

	rnd.Shuffle(len(terms), func(i, j int) {
		tmp := terms[i]
		terms[i] = terms[j]
		terms[j] = tmp
	})

	res := make(map[string]uint64, len(names))
	for i := 0; i < len(names); i++ {
		res[names[i]] = terms[i]
	}
	return res
}
