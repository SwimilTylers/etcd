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

type RackProgressAnalyzer interface {
	//InitAs initializes RackProgressAnalyzer with the specific RackProgressDescriptor forcefully.
	// If there are latent variables and buffered entries, clear it out.
	InitAs(d *RackProgressDescriptor)

	//Progress gets the latest progress since the last Progress call.
	// This operation triggers the analysis and clears out buffer.
	Progress() (*RackProgressDescriptor, error)

	//TryOfferCollector buffers Collector to RackProgressAnalyzer for analysis.
	// If analyzer does not accept the offer, return false.
	TryOfferCollector(term, termHolder uint64, c Collector) bool

	//TryOfferEntries buffers new entries to RackProgressAnalyzer for analysis.
	// If analyzer does not accept the offer, return false.
	TryOfferEntries(term, termHolder uint64, logTerm, logIndex uint64, ent []raftpb.Entry) bool

	//MatchEntryPrefix asks RackProgressAnalyzer the compatible log record metadata.
	// If 'logTerm' and 'logIndex' match the record, return true.
	// Otherwise, return false as well as matchedLogTerm and matchedLogIndex.
	// Here, matchedLogTerm = max {term|term<=logTerm},
	// matchedLogIndex = max {entry.index|entry.term == matchedLogTerm}
	// This operation triggers the analysis but will not clear out buffer.
	MatchEntryPrefix(logTerm, logIndex uint64) (bool, uint64, uint64)
}

type CollectorAnalyzer struct {
	Collector

	term    uint64
	tHolder uint64
	commit  uint64

	eRange []struct{ startIdx, term uint64 }
}

func (ca *CollectorAnalyzer) InitAs(d *RackProgressDescriptor) {
	ca.Refresh()

	ca.term = d.Term
	ca.tHolder = d.TermHolder
	ca.commit = d.Commit
	ca.eRange = make([]struct{ startIdx, term uint64 }, 0, 10)

	ca.AddEntries(d.Entries, d.LogTerm, d.LogIndex)
}

func (ca *CollectorAnalyzer) Progress() (*RackProgressDescriptor, error) {
	panic("implement me")
}

func (ca *CollectorAnalyzer) TryOfferCollector(term, termHolder uint64, c Collector) bool {
	if term < ca.term {
		return false
	}

	if term > ca.term {
		ca.term = term
		ca.tHolder = termHolder
	}

	if ca.tHolder != termHolder {
		return false
	}

	panic("")
}

func (ca *CollectorAnalyzer) TryOfferEntries(term, termHolder uint64, logTerm, logIndex uint64, ent []raftpb.Entry) bool {
	panic("implement me")
}

func (ca *CollectorAnalyzer) MatchEntryPrefix(logTerm, logIndex uint64) (bool, uint64, uint64) {
	panic("implement me")
}

func (ca *CollectorAnalyzer) refreshERange(logTerm, logIndex uint64, ent []raftpb.Entry) {

}
