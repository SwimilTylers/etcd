package draft

import (
	"go.etcd.io/etcd/draft/collector"
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
	term   uint64
	commit uint64

	compacted     *collector.MimicRaftKernelBriefCollector
	beforeCompact *collector.MimicRaftKernelCollector
	bcCTerm       uint64
	bcCTHolder    uint64

	analyzed bool

	beforeAnalysis    []*collector.EntryFragment
	beforeFingerprint map[*collector.EntryFragment]uint64
	beforeCommitted   uint64
	beforeTerm        uint64

	bufSize int
}

func NewMimicRaftKernelAnalyzer(offerBufSize int) *MimicRaftKernelAnalyzer {
	return &MimicRaftKernelAnalyzer{
		compacted:         collector.NewInitializedMimicRaftKernelBriefCollector(0, 0),
		beforeCompact:     collector.NewMimicRaftKernelCollector(),
		beforeAnalysis:    make([]*collector.EntryFragment, 0, offerBufSize),
		beforeFingerprint: make(map[*collector.EntryFragment]uint64, offerBufSize),
		bufSize:           offerBufSize,
	}
}

func (an *MimicRaftKernelAnalyzer) OfferLocalEntries(oTerm, oId, committed, prevLogTerm uint64, ent []raftpb.Entry) {
	if oTerm < an.term {
		return
	}

	an.analyzed = false

	// update term
	if oTerm > an.beforeTerm {
		an.beforeTerm = oTerm
	}

	// update committed
	if an.beforeCommitted < committed {
		an.beforeCommitted = committed
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
}

func (an *MimicRaftKernelAnalyzer) OfferRemoteEntries(oTerm, oId, committed uint64, efc collector.EntryFragmentCollector) {
	if oTerm < an.term {
		return
	}

	an.analyzed = false

	// update term
	if oTerm > an.beforeTerm {
		an.beforeTerm = oTerm
	}

	// update committed
	if an.beforeCommitted < committed {
		an.beforeCommitted = committed
	}

	if ok, fs := efc.FetchFragmentsWithStartIndex(an.commit + 1); !efc.IsNotInitialized() && ok {
		an.beforeAnalysis = append(an.beforeAnalysis, fs...)
		for _, f := range fs {
			an.beforeFingerprint[f] = oId
		}
	}
}

func (an *MimicRaftKernelAnalyzer) AnalyzeAndRemoveOffers() {
	// whenever happens, update term first
	// todo: what about the same term
	if an.beforeTerm > an.term {
		an.term = an.beforeTerm
	}

	if an.analyzed {
		return
	}

	// sorting fragments
	sort.Slice(an.beforeAnalysis, func(i, j int) bool {
		return an.beforeAnalysis[i].CTerm < an.beforeAnalysis[j].CTerm
	})

	var checkBeforeCompact = false

	if !an.beforeCompact.IsEmpty() {
		prevLogTerm, firstIndex := an.beforeCompact.PrevLogTerm(), an.beforeCompact.FirstIndex()
		loc := an.compacted.MatchIndex(prevLogTerm, firstIndex-1)
		checkBeforeCompact = loc == collector.PREV || loc == collector.WITHIN
	}

	index := len(an.beforeAnalysis) - 1

MergeInit:
	for index >= 0 {
		f := an.beforeAnalysis[index]
		switch an.compacted.MatchIndex(f.LogIndex, f.LogTerm) {
		case collector.UNDERFLOW:
			panic("underflow occurs when merging beforeAnalysis")
		case collector.PREV, collector.WITHIN:
			// reset beforeCompact
			an.beforeCompact.Refresh()
			an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex)
			an.bcCTerm = f.CTerm
			an.bcCTHolder = an.beforeFingerprint[f]

			an.mergeBeforeAnalysis(index + 1)
			an.truncateCompacted()
			break MergeInit
		case collector.OVERFLOW:
			if checkBeforeCompact {
				if an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
					an.bcCTerm = f.CTerm
					an.bcCTHolder = an.beforeFingerprint[f]

					an.mergeBeforeAnalysis(index + 1)
					break MergeInit
				}
			}
		}
	}

	an.analyzed = true
	an.removeOffers()
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

func (an *MimicRaftKernelAnalyzer) Progress() *RackProgressDescriptor {
	if !an.analyzed {
		panic("some fragments are still no analyzed")
	}

	if an.beforeCompact.IsEmpty() {
		return noProgress()
	}

	_, ent, logTerm, logIndex := an.beforeCompact.FetchAllEntries()

	if an.compacted.IsEmpty() {
		return newProgress(an.bcCTerm, an.bcCTHolder, an.commit, logTerm, logIndex, ent)
	}

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

func (an *MimicRaftKernelAnalyzer) TrySetTerm(term uint64) bool {
	if an.term > term {
		return false
	}

	an.term = term
	return true
}

func (an *MimicRaftKernelAnalyzer) GetSubLocator(compacted bool) collector.Locator {
	if !an.analyzed {
		panic("locator is unreachable before analyze finishes")
	}

	if compacted {
		return an.compacted
	} else {
		return an.beforeCompact
	}
}

func (an *MimicRaftKernelAnalyzer) analyzeWithCompacted() {
	for _, f := range an.beforeAnalysis {
		// try to anchor it in the compacted analysis
		switch an.compacted.MatchIndex(f.LogIndex, f.LogTerm) {
		case collector.UNDERFLOW:
			panic("an underflow occurs in compacted analysis")
		case collector.WITHIN, collector.PREV, collector.OVERFLOW:
			// matched in compacted analysis
			ok, loc := an.beforeCompact.TryAddEntries(f.Fragment, f.LogTerm, f.LogIndex)
			switch {
			case ok:
				an.bcCTerm = f.CTerm
				an.bcCTHolder = an.beforeFingerprint[f]
			case !ok && loc == collector.UNDERFLOW:
				// refresh beforeCompact with this fragment
				an.beforeCompact.Refresh()
				if an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
					an.bcCTerm = f.CTerm
					an.bcCTHolder = an.beforeFingerprint[f]
				}
			}
		case collector.CONFLICT:
			// mismatched in compact analysis, might matched in beforeCompact
			if !an.beforeCompact.IsEmpty() &&
				an.beforeCompact.MatchIndex(f.LogIndex, f.LogTerm) == collector.WITHIN {
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

	an.truncateCompacted()
}

func (an *MimicRaftKernelAnalyzer) analyzeWithoutCompacted() {
	for _, f := range an.beforeAnalysis {
		ok, loc := an.beforeCompact.TryAddEntries(f.Fragment, f.LogTerm, f.LogIndex)
		switch {
		case ok:
			an.bcCTerm = f.CTerm
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

func (an *MimicRaftKernelAnalyzer) mergeBeforeAnalysis(boundIndex int) {
	size := len(an.beforeAnalysis)

	for boundIndex < size {
		index := size - 1
		for index >= boundIndex {
			f := an.beforeAnalysis[index]
			if an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
				an.bcCTerm = f.CTerm
				an.bcCTHolder = an.beforeFingerprint[f]
				break
			}
			index--
		}

		if index < boundIndex {
			break
		}

		boundIndex = index + 1
	}
}

func (an *MimicRaftKernelAnalyzer) truncateCompacted() {
	if !an.beforeCompact.IsEmpty() {
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
}

func (an *MimicRaftKernelAnalyzer) updateCommit(committed uint64) {
	if committed > an.commit {
		an.commit = committed
	}
}

func (an *MimicRaftKernelAnalyzer) removeOffers() {
	if len(an.beforeAnalysis) > an.bufSize {
		an.beforeAnalysis = make([]*collector.EntryFragment, 0, an.bufSize)
	} else {
		an.beforeAnalysis = an.beforeAnalysis[:0]
	}
	an.beforeFingerprint = make(map[*collector.EntryFragment]uint64, an.bufSize)
}
