package draft

import (
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

	//TryOfferCollector buffers Collector to RackProgressAnalyzer for analysis.
	// If analyzer does not accept the offer, return false.
	TryOfferCollector(term, termHolder, latestCommit uint64, c Collector) bool

	//TryOfferEntries buffers new entries to RackProgressAnalyzer for analysis.
	// If analyzer does not accept the offer, return false.
	TryOfferEntries(term, termHolder, latestCommit, logTerm, logIndex uint64, ent []raftpb.Entry) bool

	//MatchEntryPrefix asks RackProgressAnalyzer the compatible log record metadata.
	// If 'logTerm' and 'logIndex' match the record, return true.
	// Otherwise, return false as well as matchedLogTerm and matchedLogIndex.
	// Here, matchedLogTerm = max {term|term<=logTerm},
	// matchedLogIndex = max {entry.index|entry.term == matchedLogTerm}
	// This operation triggers the analysis but will not clear out buffer.
	MatchEntryPrefix(logTerm, logIndex uint64) (bool, uint64, uint64)
}

type CACMergeType func(commit uint64, major Collector, minor []Collector, minorCommit []uint64) uint64
type CABMergeType func(s0, s1 []*CollectorBriefSegment) []*CollectorBriefSegment

//CollectorAnalyzer employs several types of Collector to analyze progress and resolve conflicts
// between different information sources. The analyzer uses a array of minor collectors to record
// the incoming offers. TryOfferEntries buffers entries directly to the first minor collector.
// TryOfferCollector appends the incoming Collector to the minor collector array. Before analysis,
// the analyzer will launch collector merging (cMerge) or brief segment merging (bMerge).
type CollectorAnalyzer struct {
	majorSeq    Collector
	minorSeq    []Collector
	minorCommit []uint64

	cachedMajorTable []*CollectorBriefSegment

	cMerge CACMergeType
	bMerge CABMergeType

	term    uint64
	tHolder uint64
	commit  uint64

	modified bool
}

func NewDefaultCollectorAnalyzer() *CollectorAnalyzer {
	return NewCollectorAnalyzer(
		NewConsecutiveEntryCollector(),
		NewConsecutiveEntryCollector(),
		CEC2EFCxMerge,
		CombineCollectorBriefSegment,
	)
}

func NewCollectorAnalyzer(majorSeq Collector, minorFirstSeq Collector, cMerge CACMergeType, bMerge CABMergeType) *CollectorAnalyzer {
	minorSeq := make([]Collector, 1, 3)
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
		return noProgress(), nil
	}

	ca.upgrade()
	_, ent, lt, li := ca.majorSeq.FetchAllEntries()
	ca.majorSeq.Refresh()

	ca.modified = false

	return newProgress(ca.term, ca.tHolder, ca.commit, lt, li, ent), nil
}

func (ca *CollectorAnalyzer) TryOfferCollector(term, termHolder, latestCommit uint64, c Collector) bool {
	if !ca.isLegalOffer(latestCommit, term) {
		return false
	}

	ca.modified = true
	ca.destroyCachedMajorTable()

	if term > ca.term {
		ca.term = term
		ca.tHolder = termHolder
	}

	ca.minorSeq = append(ca.minorSeq, c)
	ca.minorCommit = append(ca.minorCommit, latestCommit)
	return true
}

func (ca *CollectorAnalyzer) TryOfferEntries(term, termHolder, latestCommit, logTerm, logIndex uint64, ent []raftpb.Entry) bool {
	if !ca.isLegalOffer(latestCommit, term) {
		return false
	}

	ca.modified = true
	ca.destroyCachedMajorTable()

	if term > ca.term {
		ca.term = term
		ca.tHolder = termHolder
	}

	ca.minorSeq[0].AddEntries(ent, logTerm, logIndex)
	ca.minorCommit[0] = latestCommit
	return true
}

func (ca *CollectorAnalyzer) MatchEntryPrefix(logTerm, logIndex uint64) (bool, uint64, uint64) {
	if !ca.modified {
		if ca.cachedMajorTable == nil {
			ca.upgrade()
		}
		return ca.locateCacheWithPrefix(logTerm, logIndex)
	}

	ca.upgrade()
	return ca.locateCacheWithPrefix(logTerm, logIndex)
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
	ca.majorSeq.Refresh()
	ca.commit = ca.cMerge(ca.commit, ca.majorSeq, ca.minorSeq, ca.minorCommit)
	ca.cachedMajorTable = ca.bMerge(ca.cachedMajorTable, ca.majorSeq.Briefing())
}

func (ca *CollectorAnalyzer) locateCacheWithPrefix(logTerm, logIndex uint64) (bool, uint64, uint64) {
	panic("implement me")
	return false, 0, 0
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
