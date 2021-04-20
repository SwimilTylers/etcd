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

//RackProgressAnalyzer analyzes the current progress of the specific rack.
type RackProgressAnalyzer interface {
	//InitAs initializes RackProgressAnalyzer with the specific RackProgressDescriptor forcefully.
	// If there are latent variables and buffered entries, clear it out.
	InitAs(d *RackProgressDescriptor)

	//Progress gets the latest progress since the last Progress call.
	// This operation triggers the analysis and clears out buffer.
	Progress() (*RackProgressDescriptor, error)

	//MuteLastOffer mutes the last successful TryOfferCollector or TryOfferEntries. Notice that the
	// offer will not be diminished. If some new offer
	MuteLastOffer()

	//TryOfferCollector buffers EntryFragmentCollector to RackProgressAnalyzer for analysis.
	// If analyzer does not accept the offer, return false.
	TryOfferCollector(term, termHolder, latestCommit uint64, c collector.EntryFragmentCollector) bool

	TryOfferEntries(logTerm, logIndex, commit uint64, ent []raftpb.Entry) bool

	ConfirmLeadership(term, tHolder uint64) bool

	LastIndex() uint64

	Commit() uint64
}

type MimicRaftKernelAnalyzer struct {
	term    uint64
	tHolder uint64
	commit  uint64

	compacted     *collector.MimicRaftKernelBriefCollector
	beforeCompact *collector.MimicRaftKernelCollector

	beforeAnalysis  []*collector.EntryFragment
	beforeCommitted uint64
	beforeTerm      uint64
	beforeTHolder   uint64

	remoteOffer bool
	remoteJoin  bool
}

func (an *MimicRaftKernelAnalyzer) OfferLocalVote(oTerm, oTHolder uint64) {
	if oTerm < an.term {
		return
	}

	switch {
	case oTerm < an.beforeTerm:
	case oTerm == an.beforeTHolder && an.beforeTHolder != raft.None:
	default:
		an.beforeTerm = oTerm
		an.beforeTHolder = oTHolder
	}
}

func (an *MimicRaftKernelAnalyzer) OfferRemoteVote(oTerm, oTHolder uint64) {
	if oTerm < an.term {
		return
	}

	switch {
	case oTerm < an.beforeTerm:
	case oTerm == an.beforeTHolder && an.beforeTHolder != raft.None:
	default:
		an.beforeTerm = oTerm
		an.beforeTHolder = oTHolder
		an.remoteOffer = true
	}
}

func (an *MimicRaftKernelAnalyzer) OfferLocalEntries(oTerm, committed, prevLogTerm uint64, ent []raftpb.Entry) {
	if oTerm < an.term {
		return
	}

	// update term
	switch {
	case oTerm < an.beforeTerm:
	case oTerm == an.beforeTHolder && an.beforeTHolder != raft.None:
	default:
		an.beforeTerm = oTerm
		an.beforeTHolder = raft.None
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
	if an.beforeCommitted < committed {
		an.beforeCommitted = committed
	}
}

func (an *MimicRaftKernelAnalyzer) OfferRemoteEntries(oTerm, committed uint64, efc collector.EntryFragmentCollector) {
	if oTerm < an.term {
		return
	}

	// update term
	switch {
	case oTerm < an.beforeTerm:
	case oTerm == an.beforeTHolder && an.beforeTHolder != raft.None:
	default:
		an.beforeTerm = oTerm
		an.beforeTHolder = raft.None
		an.remoteOffer = true
	}

	if ok, fs := efc.FetchFragmentsWithStartIndex(an.commit + 1); !efc.IsNotInitialized() && ok {
		an.remoteOffer = true
		an.beforeAnalysis = append(an.beforeAnalysis, fs...)
		if an.beforeCommitted < committed {
			an.beforeCommitted = committed
		}
	}
}

func (an *MimicRaftKernelAnalyzer) HasRemoteOffer() bool {
	return an.remoteOffer
}

func (an *MimicRaftKernelAnalyzer) AnalyzeAndRemoveOffers() {
	// whenever happens, update term first
	if an.beforeTerm > an.term {
		an.term = an.beforeTerm
		an.tHolder = an.beforeTHolder
	}

	if len(an.beforeAnalysis) == 0 {
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

	an.beforeAnalysis = an.beforeAnalysis[:0]
	an.remoteOffer = false
}

func (an *MimicRaftKernelAnalyzer) HasRemoteOfferJoin() bool {
	if len(an.beforeAnalysis) != 0 {
		panic("some fragments are still no analyzed")
	}

	return an.remoteJoin
}

func (an *MimicRaftKernelAnalyzer) Committed() uint64 {
	if len(an.beforeAnalysis) != 0 {
		panic("some fragments are still no analyzed")
	}

	return an.commit
}

func (an *MimicRaftKernelAnalyzer) Term() uint64 {
	if len(an.beforeAnalysis) != 0 {
		panic("some fragments are still no analyzed")
	}

	return an.term
}

func (an *MimicRaftKernelAnalyzer) Progress() *RackProgressDescriptor {
	if len(an.beforeAnalysis) != 0 {
		panic("some fragments are still no analyzed")
	}

	if an.beforeCompact.IsNotInitialized() {
		return noProgress()
	}

	_, ent, logTerm, logIndex := an.beforeCompact.FetchAllEntries()

	return newProgress(an.term, an.tHolder, an.commit, logTerm, logIndex, ent)
}

func (an *MimicRaftKernelAnalyzer) RemoteProgress() *RackProgressDescriptor {
	if len(an.beforeAnalysis) != 0 {
		panic("some fragments are still left unprocessed")
	}

	if !an.remoteOffer || an.beforeCompact.IsNotInitialized() {
		return noProgress()
	}

	_, ent, logTerm, logIndex := an.beforeCompact.FetchAllEntries()

	return newProgress(an.term, an.tHolder, an.commit, logTerm, logIndex, ent)
}

func (an *MimicRaftKernelAnalyzer) Compact() {
	if an.beforeCompact.IsEmpty() {
		return
	}

	_, ent, logTerm, logIndex := an.beforeCompact.FetchAllEntries()
	an.compacted.AddEntriesToBrief(ent, logTerm, logIndex)
	an.beforeCompact.Refresh()
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
			if !ok && loc == collector.UNDERFLOW {
				// refresh beforeCompact with this fragment
				an.beforeCompact.Refresh()
				an.beforeCompact.TryAddEntries(f.Fragment, f.LogTerm, f.LogIndex)
			}
		}
	}

	if an.beforeCompact.IsEmpty() {
		return
	}

	prevLogTerm, firstIndex := an.beforeCompact.PrevLogTerm(), an.beforeCompact.FirstIndex()
	switch an.compacted.MatchIndex(prevLogTerm, firstIndex-1) {
	case collector.UNDERFLOW:
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
		if !ok && loc == collector.UNDERFLOW {
			// refresh beforeCompact with this fragment
			an.beforeCompact.Refresh()
			an.beforeCompact.TryAddEntries(f.Fragment, f.LogTerm, f.LogIndex)
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

type CACMergeType func(commit uint64, major collector.Collector, minor []collector.EntryFragmentCollector, minorCommit []uint64) uint64
type CABMergeType func(s0, s1 []*collector.BriefSegment) []*collector.BriefSegment

//CollectorAnalyzer employs several types of Collector to analyze progress and resolve conflicts
// between different information sources. The analyzer uses a array of minor collectors to record
// the incoming offers. TryOfferEntries buffers entries directly to the first minor collector.
// TryOfferCollector appends the incoming Collector to the minor collector array. Before analysis,
// the analyzer will launch collector merging (cMerge) or brief segment merging (bMerge).
type CollectorAnalyzer struct {
	majorSeq    collector.Collector
	minorSeq    []collector.EntryFragmentCollector
	minorCommit []uint64

	cachedMajorTable []*collector.BriefSegment
	compacted        *collector.MimicRaftKernelBriefCollector

	cMerge CACMergeType
	bMerge CABMergeType

	term    uint64
	tHolder uint64
	commit  uint64

	modified bool
}

func NewDefaultCollectorAnalyzer() *CollectorAnalyzer {
	return NewCollectorAnalyzer(
		collector.NewMimicRaftKernelCollector(),
		collector.NewSingleFragmentCollector(collector.NewMimicRaftKernelCollector()),
		CECEFCxMerge,
		nil,
	)
}

func NewCollectorAnalyzer(majorSeq collector.Collector, minorFirstSeq collector.EntryFragmentCollector, cMerge CACMergeType, bMerge CABMergeType) *CollectorAnalyzer {
	minorSeq := make([]collector.EntryFragmentCollector, 1, 3)
	minorSeq[0] = minorFirstSeq
	return &CollectorAnalyzer{majorSeq: majorSeq, minorSeq: minorSeq, cMerge: cMerge, bMerge: bMerge, modified: false}
}

func (ca *CollectorAnalyzer) InitAs(d *RackProgressDescriptor) {
	ca.refreshCollectors()
	ca.destroyCachedMajorTable()

	ca.term = d.Term
	ca.tHolder = d.TermHolder
	ca.commit = d.Commit

	ca.majorSeq.AddEntries(d.Entries, d.LogTerm, d.LogIndex)

	ca.modified = true
}

func (ca *CollectorAnalyzer) Progress() (*RackProgressDescriptor, error) {
	if !ca.modified {
		// clear out mute offers
		ca.refreshCollectors()
		return noProgress(), nil
	}

	ca.upgrade()
	_, ent, lt, li := ca.majorSeq.FetchAllEntries()

	ca.refreshCollectors()

	return newProgress(ca.term, ca.tHolder, ca.commit, lt, li, ent), nil
}

func (ca *CollectorAnalyzer) MuteLastOffer() {
	ca.modified = false
}

func (ca *CollectorAnalyzer) TryOfferCollector(term, termHolder, latestCommit uint64, c collector.EntryFragmentCollector) bool {
	if !ca.isLegalOffer(latestCommit, term) {
		return false
	}

	ca.modified = true

	if term > ca.term {
		ca.term = term
		ca.tHolder = termHolder
	}

	ca.minorSeq = append(ca.minorSeq, c)
	ca.minorCommit = append(ca.minorCommit, latestCommit)
	return true
}

func (ca *CollectorAnalyzer) TryOfferEntries(logTerm, logIndex, commit uint64, ent []raftpb.Entry) bool {
	ca.upgrade()

	if ca.compacted.AddEntriesToBrief(ent, logTerm, logIndex) {

	}

	return false
}

func (ca *CollectorAnalyzer) ConfirmLeadership(term, tHolder uint64) bool {
	if term < ca.term {
		return false
	}

	if term == ca.term {
		return tHolder == ca.tHolder
	}

	ca.term = term
	ca.tHolder = tHolder

	return true
}

func (ca *CollectorAnalyzer) LastIndex() uint64 {
	ca.upgrade()
	if ca.compacted.IsNotInitialized() {
		return 0
	}

	return ca.compacted.LastIndex()
}

func (ca *CollectorAnalyzer) Commit() uint64 {
	return ca.commit
}

func (ca *CollectorAnalyzer) CMerge() CACMergeType {
	return ca.cMerge
}

func (ca *CollectorAnalyzer) SetCMerge(cMerge CACMergeType) {
	ca.cMerge = cMerge
}

func (ca *CollectorAnalyzer) BMerge() CABMergeType {
	return ca.bMerge
}

func (ca *CollectorAnalyzer) SetBMerge(bMerge CABMergeType) {
	ca.bMerge = bMerge
}

func (ca *CollectorAnalyzer) refreshCollectors() {
	ca.majorSeq.Refresh()
	ca.minorSeq = ca.minorSeq[:1]
	ca.minorCommit = ca.minorCommit[:1]
	ca.minorSeq[0].Refresh()
}

func (ca *CollectorAnalyzer) destroyCachedMajorTable() {
	if ca.cachedMajorTable == nil {
		return
	}

	ca.cachedMajorTable = ca.cachedMajorTable[:0]
}

func (ca *CollectorAnalyzer) upgrade() {
	if !ca.modified {
		return
	}

	ca.commit = ca.cMerge(ca.commit, ca.majorSeq, ca.minorSeq, ca.minorCommit)
	ca.cachedMajorTable = ca.bMerge(ca.cachedMajorTable, ca.majorSeq.Briefing())
	ca.modified = false
}

func (ca *CollectorAnalyzer) isLegalOffer(commit, term uint64) bool {
	return term >= ca.term && commit >= ca.commit
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
