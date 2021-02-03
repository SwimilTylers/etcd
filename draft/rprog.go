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

	//Offer buffers new entries to RackProgressAnalyzer for analysis.
	Offer(term uint64, holder uint64, ent []raftpb.Entry) error

	//Tell asks RackProgressAnalyzer the compatible log record metadata.
	// This operation triggers the analysis but will not clear out buffer.
	Tell(logTerm, logIndex uint64) (uint64, uint64, error)
}
