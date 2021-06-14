package collector

import "go.etcd.io/etcd/raft/raftpb"

//EntryFragment represents an array of entries with consecutive indices.
type EntryFragment struct {
	LogTerm  uint64
	LogIndex uint64

	Fragment []raftpb.Entry

	CTerm uint64
}

//EntryFragmentCollector works for Entry Appending and Conflict Resolution.
type EntryFragmentCollector interface {
	//AddEntriesWithSubmitter works similar to Collector.AddEntries. However, it checks if the legitimacy
	// of the submitter term. It refreshes submitter if the addition is successful.
	AddEntriesWithSubmitter(sTerm uint64, entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool

	//FetchFragmentsWithStartIndex fetches fragments with index >= startIndex, as well as their
	// previous LogTerm and LogIndex. Return false if no such an entry.
	FetchFragmentsWithStartIndex(index uint64) (bool, []*EntryFragment)

	//FetchAllFragments fetches all fragments from internal structure. Return false if it is empty.
	FetchAllFragments() (bool, []*EntryFragment)

	Refresher
}

//SingleFragmentCollector is an implementation of EntryFragmentCollector.
type SingleFragmentCollector struct {
	cec       ConsecutiveEntryCollector
	guarantor uint64
}

func NewSingleFragmentCollector(cec ConsecutiveEntryCollector) *SingleFragmentCollector {
	return &SingleFragmentCollector{cec: cec, guarantor: 0}
}

func (c *SingleFragmentCollector) AddEntriesWithSubmitter(sTerm uint64, entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	if sTerm < c.guarantor {
		return false
	}

	if ok, _ := c.cec.TryAddEntries(entries, logTerm, logIndex); !ok {
		return false
	}

	c.guarantor = sTerm
	return true
}

func (c *SingleFragmentCollector) FetchFragmentsWithStartIndex(index uint64) (bool, []*EntryFragment) {
	if c.IsRefreshed() {
		return false, nil
	}

	ok, ent, lt, li := c.cec.FetchEntriesWithStartIndex(index)

	if !ok {
		return false, nil
	}

	return true, []*EntryFragment{{
		LogTerm:  lt,
		LogIndex: li,
		Fragment: ent,
		CTerm:    c.guarantor,
	}}
}

func (c *SingleFragmentCollector) FetchAllFragments() (bool, []*EntryFragment) {
	if c.IsRefreshed() {
		return false, nil
	}

	_, ent, lt, li := c.cec.FetchAllEntries()

	return true, []*EntryFragment{{
		LogTerm:  lt,
		LogIndex: li,
		Fragment: ent,
		CTerm:    c.guarantor,
	}}
}

func (c *SingleFragmentCollector) IsRefreshed() bool {
	return c.cec.IsRefreshed()
}

func (c *SingleFragmentCollector) Refresh() {
	c.guarantor = 0
	c.cec.Refresh()
}

//MultiFragmentsCollector is an implementation of EntryFragmentCollector.
type MultiFragmentsCollector struct {
	*LinkedListCollector
}

func NewMultiFragmentsCollector() *MultiFragmentsCollector {
	return &MultiFragmentsCollector{
		NewLinkedListCollector(true, func() Collector { return NewMimicRaftKernelCollector() }),
	}
}

func (c *MultiFragmentsCollector) AddEntriesWithSubmitter(sTerm uint64, entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	c.addEntries(false, entries, logTerm, logIndex).info = sTerm
	return true
}

//FetchFragmentsWithStartIndex fetches fragments with index >= startIndex. If part of some Fragment
// is not satisfied, truncate it. Return false if no such a Fragment.
func (c *MultiFragmentsCollector) FetchFragmentsWithStartIndex(index uint64) (bool, []*EntryFragment) {
	if c.IsRefreshed() {
		return false, nil
	}

	frag := c.fetchFragments(index)

	if len(frag) == 0 {
		return false, nil
	}

	return true, frag
}

//FetchAllFragments fetches all fragments from the list. Return false if it is empty.
func (c *MultiFragmentsCollector) FetchAllFragments() (bool, []*EntryFragment) {
	if c.IsRefreshed() {
		return false, nil
	}

	return true, c.fetchFragments(0)
}

func (c *MultiFragmentsCollector) fetchFragments(index uint64) []*EntryFragment {
	var result []*EntryFragment
	needle := c.head

	for needle != nil {
		if ok, ent, logTerm, logIndex := needle.FetchEntriesWithStartIndex(index); ok {
			result = append(result, &EntryFragment{
				LogTerm:  logTerm,
				LogIndex: logIndex,
				Fragment: ent,
				CTerm:    needle.info,
			})
		}
		needle = needle.next
	}

	return result
}
