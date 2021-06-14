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

type AnalysisPolicy uint8

const (
	UpdateCommittedOnly  AnalysisPolicy = 0
	MatchFirstFragment   AnalysisPolicy = 1
	MatchLastFragment    AnalysisPolicy = 2
	StrictlyMatchFirst   AnalysisPolicy = 3
	AccordingToCompacted AnalysisPolicy = 4
	IgnoreCompacted      AnalysisPolicy = 5
)

type Analyzer interface {
	TrySetTerm(term uint64) bool
	OfferLocalEntries(oTerm, oId, committed, prevLogTerm uint64, ent []raftpb.Entry)
	OfferRemoteEntries(oTerm, oId, committed uint64, efc collector.EntryFragmentCollector)

	AnalyzeAndRemoveOffers(policy AnalysisPolicy)
	Progress() *RackProgressDescriptor
	Term() uint64
	Committed() uint64
	Analyzed() bool
}

type MimicRaftKernelAnalyzer struct {
	term   uint64 //term is the currentTerm of analyzer. It will be updated by beforeTerm other than bcCTerm during analysis.
	commit uint64 //commit is the committed index of analyzer.

	compacted     *collector.MimicRaftKernelBriefCollector //compacted is the replication log successfully synchronized to raft kernel.
	beforeCompact *collector.MimicRaftKernelCollector      //beforeCompact is the replication log yet not successfully synchronized to raft kernel.
	bcCTerm       uint64                                   //bcCTerm is CTerm of the last fragment in beforeAnalysis that successfully added to beforeCompact.
	bcCTHolder    uint64                                   //bcCTHolder is the holder to bcCTerm. If there are several holders, choose the one who offers the latest entry.

	analyzed bool //analyzed indicates whether the offerings are consumed by analysis.

	beforeAnalysis    []*collector.EntryFragment          //beforeAnalysis buffers offered fragments for further analysis.
	beforeFingerprint map[*collector.EntryFragment]uint64 //beforeFingerprint marks ids of the fragment offerings.
	beforeCommitted   uint64                              //beforeCommitted records the greatest committed index ever offered.
	beforeTerm        uint64                              //beforeTerm records the greatest currentTerm ever offered. It might be greater than bcCTerm even after analysis.

	bufSize int //bufSize defines the default capacity of buffers.

	rbFragments map[uint64][]*collector.EntryFragment //rbFragments buffers beforeAnalysis for rollback operations.
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

//CloneMimicRaftKernelAnalyzer offers a deep copy of MimicRaftKernelAnalyzer
func CloneMimicRaftKernelAnalyzer(an *MimicRaftKernelAnalyzer) *MimicRaftKernelAnalyzer {
	ba := make([]*collector.EntryFragment, 0, cap(an.beforeAnalysis))
	bf := make(map[*collector.EntryFragment]uint64, cap(an.beforeAnalysis))
	for f, id := range an.beforeFingerprint {
		bf[f] = id
	}

	return &MimicRaftKernelAnalyzer{
		term:              an.term,
		commit:            an.commit,
		compacted:         collector.CloneMimicRaftKernelBriefCollector(an.compacted),
		beforeCompact:     collector.CloneMimicRaftKernelCollector(an.beforeCompact),
		bcCTerm:           an.bcCTerm,
		bcCTHolder:        an.bcCTHolder,
		analyzed:          an.analyzed,
		beforeAnalysis:    append(ba, an.beforeAnalysis...),
		beforeFingerprint: bf,
		beforeCommitted:   an.beforeCommitted,
		beforeTerm:        an.beforeTerm,
		bufSize:           an.bufSize,
		rbFragments:       nil,
	}
}

//CopyMimicRaftKernelAnalyzer conducts a shallow copy of MimicRaftKernelAnalyzer
func CopyMimicRaftKernelAnalyzer(dst, src *MimicRaftKernelAnalyzer) {
	dst.term = src.term
	dst.commit = src.commit
	dst.compacted = src.compacted
	dst.beforeCompact = src.beforeCompact
	dst.bcCTerm = src.bcCTerm
	dst.bcCTHolder = src.bcCTHolder
	dst.analyzed = src.analyzed
	dst.beforeAnalysis = src.beforeAnalysis
	dst.beforeFingerprint = src.beforeFingerprint
	dst.beforeCommitted = src.beforeCommitted
	dst.beforeTerm = src.beforeTerm
	dst.bufSize = src.bufSize
	dst.rbFragments = nil
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

	if ok, fs := efc.FetchFragmentsWithStartIndex(an.commit + 1); !efc.IsRefreshed() && ok {
		an.beforeAnalysis = append(an.beforeAnalysis, fs...)
		for _, f := range fs {
			an.beforeFingerprint[f] = oId
		}
	}
}

func (an *MimicRaftKernelAnalyzer) AnalyzeAndRemoveOffers(policy AnalysisPolicy) {
	// whenever happens, update term first
	// todo: what about the same term
	if an.beforeTerm > an.term {
		an.term = an.beforeTerm
	}

	if an.analyzed {
		return
	}

	switch policy {
	case UpdateCommittedOnly:
		an.alignBeforeCommitted()
	case MatchFirstFragment:
		an.sortBeforeAnalysis()
		an.analyzePolicyFirstMatch(false)
	case MatchLastFragment:
		an.sortBeforeAnalysis()
		an.analyzePolicyLastMatch()
	case StrictlyMatchFirst:
		an.sortBeforeAnalysis()
		an.analyzePolicyFirstMatch(true)
	case AccordingToCompacted:
		an.sortBeforeAnalysis()
		an.analyzeWithCompacted()
	case IgnoreCompacted:
		an.sortBeforeAnalysis()
		an.analyzeWithoutCompacted()
	default:
		panic("undefined policy")
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

func (an *MimicRaftKernelAnalyzer) Analyzed() bool {
	return an.analyzed
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

func (an *MimicRaftKernelAnalyzer) UncheckedProgress() (*RackProgressDescriptor, *RackProgressDescriptor) {
	_, ent, logTerm, logIndex := an.beforeCompact.FetchAllEntries()
	if an.analyzed {
		return newProgress(an.bcCTerm, an.bcCTHolder, an.commit, logTerm, logIndex, ent),
			noProgress()
	} else {
		return newProgress(an.bcCTerm, an.bcCTHolder, an.commit, logTerm, logIndex, ent),
			newProgress(an.beforeTerm, raft.None, an.beforeCommitted, 0, 0, nil)
	}
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

func (an *MimicRaftKernelAnalyzer) DropOffers() bool {
	if an.analyzed {
		return false
	}

	an.removeOffers()
	an.analyzed = true

	return true
}

func (an *MimicRaftKernelAnalyzer) PrepareRollback() bool {
	if an.analyzed {
		return false
	}

	if an.rbFragments != nil {
		panic("duplicated rollback")
	}

	an.rbFragments = make(map[uint64][]*collector.EntryFragment, an.bufSize)
	for _, f := range an.beforeAnalysis {
		id := an.beforeFingerprint[f]
		an.rbFragments[id] = append(an.rbFragments[id], f)
	}

	// we do not have to rollback term and committed!

	//an.beforeTerm = an.term
	//an.beforeCommitted = an.commit

	an.removeOffers()
	an.analyzed = true

	return true
}

func (an *MimicRaftKernelAnalyzer) RollbackOffers(id uint64, efc collector.EntryFragmentCollector) {
	if len(an.rbFragments) == 0 {
		return
	}

	defer func() {
		delete(an.rbFragments, id)
		if len(an.rbFragments) == 0 {
			// release rollback-related resources
			an.rbFragments = nil
		}
	}()

	if fs, ok := an.rbFragments[id]; ok {
		for _, f := range fs {
			efc.AddEntriesWithSubmitter(f.CTerm, f.Fragment, f.LogTerm, f.LogIndex)
		}
	}
}

func (an *MimicRaftKernelAnalyzer) RollbackAll() map[uint64][]*collector.EntryFragment {
	res := an.rbFragments
	an.rbFragments = nil
	return res
}

func (an *MimicRaftKernelAnalyzer) sortBeforeAnalysis() {
	// sorting fragments
	sort.Slice(an.beforeAnalysis, func(i, j int) bool {
		var ai = an.beforeAnalysis[i]
		var aj = an.beforeAnalysis[j]

		// comparing guarantor, ascending
		if ai.CTerm != aj.CTerm {
			return ai.CTerm < aj.CTerm
		}

		// comparing lastIndex, ascending
		lastI := ai.Fragment[len(ai.Fragment)-1]
		lastJ := aj.Fragment[len(aj.Fragment)-1]

		return lastI.Index < lastJ.Index
	})
}

func (an *MimicRaftKernelAnalyzer) analyzePolicyFirstMatch(strict bool) {
	var checkBeforeCompact = false

	if !an.beforeCompact.IsEmpty() {
		prevLogTerm, firstIndex := an.beforeCompact.PrevLogTerm(), an.beforeCompact.FirstIndex()
		loc := an.compacted.MatchIndex(firstIndex-1, prevLogTerm)
		checkBeforeCompact = loc == collector.PREV || loc == collector.WITHIN
	}

	size := len(an.beforeAnalysis)
	index := 0

MergeInit:
	for index < size {
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

			an.mergeBeforeAnalysisForward(index+1, strict)
			an.truncateCompacted()
			break MergeInit
		case collector.OVERFLOW:
			if checkBeforeCompact {
				if an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
					an.bcCTerm = f.CTerm
					an.bcCTHolder = an.beforeFingerprint[f]

					an.mergeBeforeAnalysisForward(index+1, strict)
					an.truncateCompacted()
					break MergeInit
				}
			}
		}
		index++
	}

	an.alignBeforeCommitted()
}

//deprecated
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

	an.alignBeforeCommitted()
}

//deprecated
func (an *MimicRaftKernelAnalyzer) analyzeWithCompacted() {
	for _, f := range an.beforeAnalysis {
		// try to anchor it in the compacted analysis
		switch an.compacted.MatchIndex(f.LogIndex, f.LogTerm) {
		case collector.UNDERFLOW:
			panic("an underflow occurs in compacted analysis")
		case collector.WITHIN, collector.PREV:
			an.beforeCompact.Refresh()
			an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex)
			an.bcCTerm = f.CTerm
			an.bcCTHolder = an.beforeFingerprint[f]

			an.truncateCompacted()
		case collector.OVERFLOW:
			// matched in compacted analysis
			if an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
				an.bcCTerm = f.CTerm
				an.bcCTHolder = an.beforeFingerprint[f]
			}
		}
	}

	an.truncateCompacted()
	an.alignBeforeCommitted()
}

func (an *MimicRaftKernelAnalyzer) analyzePolicyLastMatch() {
	var checkBeforeCompact = false

	if !an.beforeCompact.IsEmpty() {
		prevLogTerm, firstIndex := an.beforeCompact.PrevLogTerm(), an.beforeCompact.FirstIndex()
		loc := an.compacted.MatchIndex(firstIndex-1, prevLogTerm)
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

			an.mergeBeforeAnalysisBackward(index + 1)
			an.truncateCompacted()
			break MergeInit
		case collector.OVERFLOW:
			if checkBeforeCompact {
				if an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
					an.bcCTerm = f.CTerm
					an.bcCTHolder = an.beforeFingerprint[f]

					an.mergeBeforeAnalysisBackward(index + 1)
					an.truncateCompacted()
					break MergeInit
				}
			}
		}
		index--
	}

	an.alignBeforeCommitted()
}

func (an *MimicRaftKernelAnalyzer) mergeBeforeAnalysisForward(from int, strict bool) {
	size := len(an.beforeAnalysis)

	if strict {
		for from <= size {
			picked := an.beforeAnalysis[from-1]
			index := from
			for index < size {
				f := an.beforeAnalysis[index]
				if f.LogIndex > picked.LogIndex &&
					an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
					an.bcCTerm = f.CTerm
					an.bcCTHolder = an.beforeFingerprint[f]
					break
				}
				index++
			}

			if index == size {
				break
			}

			from = index + 1
		}
	} else {
		for i := from; i < size; i++ {
			f := an.beforeAnalysis[i]
			if ok, loc := an.beforeCompact.TryAddEntries(f.Fragment, f.LogTerm, f.LogIndex); ok {
				an.bcCTerm = f.CTerm
				an.bcCTHolder = an.beforeFingerprint[f]
			} else if loc == collector.UNDERFLOW {
				switch an.compacted.MatchIndex(f.LogIndex, f.LogTerm) {
				case collector.UNDERFLOW:
					panic("underflow occurs when merging beforeAnalysis")
				case collector.PREV, collector.WITHIN:
					// refresh beforeCompact
					an.beforeCompact.Refresh()
					an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex)
					an.bcCTerm = f.CTerm
					an.bcCTHolder = an.beforeFingerprint[f]
				}
			}

		}
	}
}

func (an *MimicRaftKernelAnalyzer) mergeBeforeAnalysisBackward(from int) {
	size := len(an.beforeAnalysis)

	for from < size {
		index := size - 1
		for index >= from {
			f := an.beforeAnalysis[index]
			if an.beforeCompact.AddEntries(f.Fragment, f.LogTerm, f.LogIndex) {
				an.bcCTerm = f.CTerm
				an.bcCTHolder = an.beforeFingerprint[f]
				break
			}
			index--
		}

		if index < from {
			break
		}

		from = index + 1
	}
}

func (an *MimicRaftKernelAnalyzer) truncateCompacted() {
	if !an.beforeCompact.IsEmpty() {
		prevLogTerm, firstIndex := an.beforeCompact.PrevLogTerm(), an.beforeCompact.FirstIndex()
		switch an.compacted.MatchIndex(firstIndex-1, prevLogTerm) {
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

func (an *MimicRaftKernelAnalyzer) alignBeforeCommitted() {
	if an.beforeCompact.IsEmpty() {
		an.updateCommit(an.beforeCommitted)
	} else {
		lastIndex := an.beforeCompact.LastIndex()
		if an.beforeCommitted < lastIndex {
			an.updateCommit(an.beforeCommitted)
		} else {
			an.updateCommit(lastIndex)
		}
		an.beforeCommitted = an.commit
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
