package draft

import (
	"go.etcd.io/etcd/draft/collector"
	"go.etcd.io/etcd/raft/raftpb"
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

	TryOfferEntries(logTerm, logIndex, commit uint64, ent []raftpb.Entry) (bool, uint64)

	ConfirmLeadership(term, tHolder uint64) bool

	LastIndex() uint64

	Commit() uint64
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
	compacted        *collector.BriefSegmentCollector

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

func (ca *CollectorAnalyzer) TryOfferEntries(logTerm, logIndex, commit uint64, ent []raftpb.Entry) (bool, uint64) {
	ca.upgrade()

	if ok, lastNewIndex := ca.compacted.AbsorbEntries(logTerm, logIndex, ent); ok {
		if lastNewIndex < commit {
			ca.commit = lastNewIndex
		} else {
			ca.commit = commit
		}
		ca.compacted.SetCommit(ca.commit)
		return true, lastNewIndex
	}

	return false, 0
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
	if ca.compacted.IsInitialized() {
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
