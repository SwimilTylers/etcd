package draft

import (
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"sort"
)

type RackProgressDescriptor struct {
	NoProgress bool

	Term       uint64
	TermHolder uint64

	LogTerm  uint64
	LogIndex uint64
	Commit   uint64
	Entries  []raftpb.Entry
}

func noProgress() *RackProgressDescriptor {
	return &RackProgressDescriptor{NoProgress: true}
}

func newProgress(term, tHolder, commit, logTerm, logIndex uint64, ent []raftpb.Entry) *RackProgressDescriptor {
	return &RackProgressDescriptor{
		NoProgress: false,
		Term:       term,
		TermHolder: tHolder,
		LogTerm:    logTerm,
		LogIndex:   logIndex,
		Commit:     commit,
		Entries:    ent,
	}
}

type MimicRaftKernelAnalyzer struct {
	term    uint64
	tHolder uint64
	commit  uint64

	compacted     *collector.MimicRaftKernelBriefCollector
	beforeCompact *collector.MimicRaftKernelCollector
	bcCTerm       uint64
	bcCTHolder    uint64

	analyzed bool

	beforeAnalysis    []*collector.EntryFragment
	beforeFingerprint map[*collector.EntryFragment]uint64
	beforeCommitted   uint64
	beforeTerm        uint64
	beforeTHolder     uint64
}

func (an *MimicRaftKernelAnalyzer) OfferLocalEntries(oTerm, oId, committed, prevLogTerm uint64, ent []raftpb.Entry) {
	if oTerm < an.term {
		return
	}

	an.analyzed = false

	// update term
	switch {
	case oTerm < an.beforeTerm:
	case oTerm == an.beforeTHolder && an.beforeTHolder != raft.None:
	default:
		an.beforeTerm = oTerm
		an.beforeTHolder = oId
	}

	if len(ent) == 0 {
		return
	}

	if ent[0].Index <= an.commit {
		// filter out stale messages
		return
	}

	f := &collector.EntryFragment{
		LogTerm:  prevLogTerm,
		LogIndex: ent[0].Index - 1,
		Fragment: ent,
		CTerm:    oTerm,
	}

	an.beforeAnalysis = append(an.beforeAnalysis, f)
	an.beforeFingerprint[f] = oId
	if an.beforeCommitted < committed {
		an.beforeCommitted = committed
	}
}

func (an *MimicRaftKernelAnalyzer) OfferRemoteEntries(oTerm, oId, committed uint64, efc collector.EntryFragmentCollector) {
	if oTerm < an.term {
		return
	}

	an.analyzed = false

	// update term
	switch {
	case oTerm < an.beforeTerm:
	case oTerm == an.beforeTHolder && an.beforeTHolder != raft.None:
	default:
		an.beforeTerm = oTerm
		an.beforeTHolder = oId
	}

	if ok, fs := efc.FetchFragmentsWithStartIndex(an.commit + 1); !efc.IsNotInitialized() && ok {
		an.beforeAnalysis = append(an.beforeAnalysis, fs...)
		for _, f := range fs {
			an.beforeFingerprint[f] = oId
		}
		if an.beforeCommitted < committed {
			an.beforeCommitted = committed
		}
	}
}

func (an *MimicRaftKernelAnalyzer) AnalyzeAndRemoveOffers() {
	// whenever happens, update term first
	if an.beforeTerm > an.term {
		an.term = an.beforeTerm
		an.tHolder = an.beforeTHolder
	}

	if an.analyzed {
		return
	}

	// sorting fragments
	sort.Slice(an.beforeAnalysis, func(i, j int) bool {
		return an.beforeAnalysis[i].CTerm < an.beforeAnalysis[j].CTerm
	})

	if an.compacted.IsEmpty() || an.compacted.IsNotInitialized() {
		an.analyzeWithoutCompacted()
	} else {
		an.analyzeWithCompacted()
	}

	an.analyzed = true
	an.beforeAnalysis = an.beforeAnalysis[:0]
	an.beforeFingerprint = make(map[*collector.EntryFragment]uint64)
}

func (an *MimicRaftKernelAnalyzer) Committed() uint64 {
	if !an.analyzed {
		panic("some fragments are still no analyzed")
	}

	return an.commit
}

func (an *MimicRaftKernelAnalyzer) Term() uint64 {
	if !an.analyzed {
		panic("some fragments are still no analyzed")
	}

	return an.term
}

func (an *MimicRaftKernelAnalyzer) TermAndTHolder() (uint64, uint64) {
	if !an.analyzed {
		panic("some fragments are still no analyzed")
	}

	return an.term, an.tHolder
}

func (an *MimicRaftKernelAnalyzer) Progress() *RackProgressDescriptor {
	if !an.analyzed {
		panic("some fragments are still no analyzed")
	}

	if an.beforeCompact.IsEmpty() {
		return noProgress()
	}

	_, ent, logTerm, logIndex := an.beforeCompact.FetchAllEntries()

	switch an.compacted.MatchIndex(logIndex, logTerm) {
	case collector.UNDERFLOW:
		panic("underflow occurs when delivering progress")
	case collector.PREV, collector.WITHIN:
		return newProgress(an.bcCTerm, an.bcCTHolder, an.commit, logTerm, logIndex, ent)
	default:
		return noProgress()
	}
}

func (an *MimicRaftKernelAnalyzer) UncheckedProgress() *RackProgressDescriptor {
	if !an.analyzed {
		panic("some fragments are still no analyzed")
	}

	if an.beforeCompact.IsEmpty() {
		return noProgress()
	}

	_, ent, logTerm, logIndex := an.beforeCompact.FetchAllEntries()

	return newProgress(an.bcCTerm, an.bcCTHolder, an.commit, logTerm, logIndex, ent)
}

func (an *MimicRaftKernelAnalyzer) Compact() {
	if an.beforeCompact.IsEmpty() {
		return
	}

	_, ent, logTerm, logIndex := an.beforeCompact.FetchAllEntries()
	an.compacted.AddEntriesToBrief(ent, logTerm, logIndex)
	an.beforeCompact.Refresh()
}

func (an *MimicRaftKernelAnalyzer) CompactBefore(index uint64) collector.Location {
	if an.beforeCompact.IsEmpty() {
		return collector.UNDERFLOW
	}

	location, _ := an.beforeCompact.LocateIndex(index)

	switch location {
	case collector.CONFLICT:
		panic("detect conflict during locating index in beforeCompact")
	case collector.WITHIN:
		_, nEnt, nLogTerm, nLogIndex := an.beforeCompact.FetchEntriesWithStartIndex(index)
		_, aEnt, aLogTerm, aLogIndex := an.beforeCompact.FetchAllEntries()
		aheadLen := len(aEnt) - len(nEnt)
		aEnt = aEnt[:aheadLen]
		an.compacted.AddEntriesToBrief(aEnt, aLogTerm, aLogIndex)
		an.beforeCompact.Refresh()
		an.beforeCompact.AddEntries(nEnt, nLogTerm, nLogIndex)
	case collector.OVERFLOW:
		_, aEnt, aLogTerm, aLogIndex := an.beforeCompact.FetchAllEntries()
		an.compacted.AddEntriesToBrief(aEnt, aLogTerm, aLogIndex)
		an.beforeCompact.Refresh()
	}

	return location
}

func (an *MimicRaftKernelAnalyzer) MatchIndexInCompact(index, term uint64) (bool, collector.Location) {
	if an.compacted.IsEmpty() {
		return false, collector.OVERFLOW
	}

	return true, an.compacted.MatchIndex(index, term)
}

func (an *MimicRaftKernelAnalyzer) MatchIndex(index, term uint64) collector.Location {
	if an.IsEmpty() {
		panic("unable to access empty records")
	}

	if an.compacted.IsEmpty() {
		return an.beforeCompact.MatchIndex(index, term)
	}

	if an.beforeCompact.IsEmpty() {
		return an.compacted.MatchIndex(index, term)
	}

	res := an.compacted.MatchIndex(index, term)
	switch res {
	case collector.UNDERFLOW:
		panic("underflow occurs when matching index")
	case collector.OVERFLOW:
		res = an.beforeCompact.MatchIndex(index, term)
		if res == collector.UNDERFLOW {
			panic("there is a gap between compacted and beforeCompact")
		}
		return res
	default:
		return res
	}
}

func (an *MimicRaftKernelAnalyzer) LocateIndex(index uint64) (collector.Location, uint64) {
	if an.IsEmpty() {
		panic("unable to access empty records")
	}

	if an.compacted.IsEmpty() {
		return an.beforeCompact.LocateIndex(index)
	}

	if an.beforeCompact.IsEmpty() {
		return an.compacted.LocateIndex(index)
	}

	res, term := an.compacted.LocateIndex(index)
	switch res {
	case collector.UNDERFLOW:
		panic("underflow occurs when matching index")
	case collector.OVERFLOW:
		res, term = an.beforeCompact.LocateIndex(index)
		if res == collector.UNDERFLOW {
			panic("there is a gap between compacted and beforeCompact")
		}
		return res, term
	default:
		return res, term
	}
}

func (an *MimicRaftKernelAnalyzer) PrevLogTerm() uint64 {
	if an.IsEmpty() {
		panic("unable to access empty records")
	}

	if an.compacted.IsEmpty() {
		return an.beforeCompact.PrevLogTerm()
	}

	return an.compacted.PrevLogTerm()
}

func (an *MimicRaftKernelAnalyzer) FirstIndex() uint64 {
	if an.IsEmpty() {
		panic("unable to access empty records")
	}

	if an.compacted.IsEmpty() {
		return an.beforeCompact.FirstIndex()
	}

	return an.compacted.FirstIndex()
}

func (an *MimicRaftKernelAnalyzer) LastIndex() uint64 {
	if an.IsEmpty() {
		panic("unable to access empty records")
	}

	if an.beforeCompact.IsEmpty() {
		return an.compacted.LastIndex()
	}

	return an.beforeCompact.LastIndex()
}

func (an *MimicRaftKernelAnalyzer) IsEmpty() bool {
	return an.compacted.IsEmpty() && an.beforeCompact.IsEmpty()
}

func (an *MimicRaftKernelAnalyzer) analyzeWithCompacted() {
	for _, f := range an.beforeAnalysis {
		// try to anchor it in the compacted analysis
		switch an.compacted.MatchIndex(f.LogIndex, f.LogTerm) {
		case collector.UNDERFLOW:
			panic("an underflow occurs in compacted analysis")
		case collector.WITHIN, collector.PREV, collector.OVERFLOW:
			// matched in compacted analysis, resize the
			ok, loc := an.beforeCompact.TryAddEntries(f.Fragment, f.LogTerm, f.LogIndex)
			switch {
			case ok:
				an.bcCTHolder = an.beforeFingerprint[f]
			case !ok && loc == collector.UNDERFLOW:
				// refresh beforeCompact with this fragment
				an.beforeCompact.Refresh()
				if an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
					an.bcCTerm = f.CTerm
					an.bcCTHolder = an.beforeFingerprint[f]
				}
			}
		}
	}

	if an.beforeCompact.IsEmpty() {
		return
	}

	prevLogTerm, firstIndex := an.beforeCompact.PrevLogTerm(), an.beforeCompact.FirstIndex()
	switch an.compacted.MatchIndex(prevLogTerm, firstIndex-1) {
	case collector.UNDERFLOW:
		// beforeCompact has more advanced index
		panic("an underflow occurs in consistency check")
	case collector.PREV, collector.WITHIN:
		// resize compacted to fit beforeCompact
		an.compacted.ResizeBriefToIndex(firstIndex - 1)
		lastIndex := an.beforeCompact.LastIndex()
		if an.beforeCommitted < lastIndex {
			an.updateCommit(an.beforeCommitted)
		} else {
			an.updateCommit(lastIndex)
		}
		an.beforeCommitted = an.commit
	case collector.CONFLICT:
		panic("an conflict occurs in consistency check")
	}
}

func (an *MimicRaftKernelAnalyzer) analyzeWithoutCompacted() {
	for _, f := range an.beforeAnalysis {
		ok, loc := an.beforeCompact.TryAddEntries(f.Fragment, f.LogTerm, f.LogIndex)
		switch {
		case ok:
			an.bcCTHolder = an.beforeFingerprint[f]
		case !ok && loc == collector.UNDERFLOW:
			// refresh beforeCompact with this fragment
			an.beforeCompact.Refresh()
			if an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
				an.bcCTerm = f.CTerm
				an.bcCTHolder = an.beforeFingerprint[f]
			}
		}
	}

	if an.beforeCompact.IsEmpty() {
		return
	}

	lastIndex := an.beforeCompact.LastIndex()
	if an.beforeCommitted < lastIndex {
		an.updateCommit(an.beforeCommitted)
	} else {
		an.updateCommit(lastIndex)
	}
	an.beforeCommitted = an.commit
}

func (an *MimicRaftKernelAnalyzer) updateCommit(committed uint64) {
	if committed > an.commit {
		an.commit = committed
	}
}
