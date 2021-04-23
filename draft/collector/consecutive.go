package collector

import "go.etcd.io/etcd/raft/raftpb"

type Location uint8

const (
	UNDERFLOW Location = 0x0
	PREV      Location = 0x1
	WITHIN    Location = 0x2
	OVERFLOW  Location = 0x4
	CONFLICT  Location = 0x8
)

type Locator interface {
	//MatchIndex compares the given tuple to its own records.
	//	1. UNDERFLOW: index is below the prevLogIndex of the entry array
	//	2. PREV: (index, term) matches (prevLogIndex, prevLogTerm)
	//	3. WITHIN: find the same entry within the entry array
	//	4. OVERFLOW: index is beyond the maximum index recorded
	//	5. CONFLICT: find the conflict record in the entry array or in the tuple (prevLogIndex, prevLogTerm)
	MatchIndex(index, term uint64) Location

	LocateIndex(index uint64) (Location, uint64)

	PrevLogTerm() uint64
	FirstIndex() uint64
	LastIndex() uint64

	//IsEmpty checks if the entry array is empty. To be mentioned, an empty array might have a prefix of
	// (prevLogIndex, prevLogTerm).
	IsEmpty() bool
}

//ConsecutiveEntryCollector is a subtype of Collector. The entry it contains MUST be consecutively increasing.
type ConsecutiveEntryCollector interface {
	//TryAddEntries works similar to Collector.AddEntries. If failed to add, it will return Location for
	// further diagnosis:
	//	1. UNDERFLOW: logIndex is below the prevLogIndex of the entry array
	//	2. OVERFLOW: logIndex is above index of the last entry
	//	3. CONFLICT: find the conflict record in the entry array or in the tuple (prevLogIndex, prevLogTerm)
	TryAddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) (bool, Location)

	//EntrySize returns the size of entry array
	EntrySize() int
	GetLatestTerm() (bool, uint64)

	Locator
	EntryFetcher
	Briefer
	Refresher
}

//MimicRaftKernelCollector is an implementation of ConsecutiveEntryCollector. It maintains
// an entry array with consecutive indices. During the process of AddEntries, it might reinitialize
// the internal array or truncate the internal array. If new entries are not appendable even
// after those steps, the collector will not take in these entries for the maintenance of
// consecutive-ness.
type MimicRaftKernelCollector struct {
	logTerm   uint64
	logIndex  uint64
	nextIndex uint64

	copied  bool
	content []raftpb.Entry

	cachedTable []struct {
		term        uint64
		first, last int
	}

	initialized bool
}

func NewMimicRaftKernelCollector() *MimicRaftKernelCollector {
	return &MimicRaftKernelCollector{copied: false, initialized: false}
}

func (c *MimicRaftKernelCollector) AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	if c.IsNotInitialized() {
		c.init(entries, logTerm, logIndex)
		return true
	}

	ok, _ := c.mimic(entries, logTerm, logIndex)
	return ok
}

func (c *MimicRaftKernelCollector) FetchEntries(term uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsNotInitialized() {
		return false, nil, 0, 0
	}

	cLen := len(c.content)

	ok, idx := c.locateEntryWithTerm(term, 0, cLen)

	if !ok {
		return false, nil, 0, 0
	}

	_, left := c.locateFirstEntryWithTerm(term, 0, idx+1)
	_, right := c.locateLastEntryWithTerm(term, idx, cLen)

	if left == 0 {
		return true, c.content[:right+1], c.logTerm, c.logIndex
	}

	last := c.content[left-1]

	return true, c.content[left : right+1], last.Term, last.Index
}

func (c *MimicRaftKernelCollector) FetchEntriesWithStartIndex(index uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsNotInitialized() {
		return false, nil, 0, 0
	}

	if index <= c.logIndex+1 {
		return true, c.content, c.logTerm, c.logIndex
	}

	if index >= c.nextIndex {
		return false, nil, 0, 0
	}

	_, idx := c.locateEntryWithIndex(index)
	before := &c.content[idx-1]
	return true, c.content[idx:], before.Term, before.Index
}

func (c *MimicRaftKernelCollector) FetchAllEntries() (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsNotInitialized() {
		return false, nil, 0, 0
	}

	return true, c.content, c.logTerm, c.logIndex
}

func (c *MimicRaftKernelCollector) Refresh() {
	if c.IsNotInitialized() {
		return
	}

	c.content = nil
	c.copied = false
	c.initialized = false
	c.destroyCachedTable()
}

func (c *MimicRaftKernelCollector) Briefing() []*BriefSegment {
	if c.IsNotInitialized() || c.IsEmpty() {
		return nil
	}

	var result []*BriefSegment
	left := 0
	logTerm := c.logTerm
	cLen := len(c.content)

	c.destroyCachedTable()
	ct := c.cachedTable
	c.cachedTable = nil

	for left < cLen {
		term := c.content[left].Term
		_, right := c.locateLastEntryWithTerm(term, left, cLen)
		result = append(result, &BriefSegment{
			Term:        term,
			PrevLogTerm: logTerm,
			FirstIndex:  c.content[left].Index,
			LastIndex:   c.content[right].Index,
		})
		ct = append(ct, struct {
			term        uint64
			first, last int
		}{term: term, first: left, last: right})
		logTerm = term
		left = right + 1
	}

	c.cachedTable = ct

	return result
}

func (c *MimicRaftKernelCollector) IsNotInitialized() bool {
	return !c.initialized
}

//TryAddEntries of MimicRaftKernelCollector mimics the behavior of raft kernel
func (c *MimicRaftKernelCollector) TryAddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) (bool, Location) {
	if c.IsNotInitialized() {
		c.init(entries, logTerm, logIndex)
		return true, PREV
	}

	ok, loc := c.mimic(entries, logTerm, logIndex)
	return ok, loc
}

func (c *MimicRaftKernelCollector) MatchIndex(index, term uint64) Location {
	if c.IsNotInitialized() {
		panic("not initialized")
	}

	if c.logIndex > index {
		// underflow, quit
		return UNDERFLOW
	} else if c.logIndex == index {
		if c.logTerm != term {
			// inconsistent, quit
			return CONFLICT
		} else {
			return PREV
		}
	} else if ok, idx := c.locateEntryWithIndex(index); !ok {
		// overflow, quit
		return OVERFLOW
	} else {
		if c.content[idx].Term != term {
			// inconsistent, quit
			return CONFLICT
		} else {
			return WITHIN
		}
	}
}

func (c *MimicRaftKernelCollector) LocateIndex(index uint64) (Location, uint64) {
	if c.IsNotInitialized() {
		panic("not initialized")
	}

	if c.logIndex > index {
		// underflow, quit
		return UNDERFLOW, 0
	} else if c.logIndex == index {
		return PREV, c.logTerm
	} else if ok, idx := c.locateEntryWithIndex(index); !ok {
		// overflow, quit
		return OVERFLOW, 0
	} else {
		return WITHIN, c.content[idx].Term
	}
}

func (c *MimicRaftKernelCollector) PrevLogTerm() uint64 {
	if c.IsNotInitialized() {
		panic("not initialized")
	}
	return c.logTerm
}

func (c *MimicRaftKernelCollector) FirstIndex() uint64 {
	if c.IsNotInitialized() || c.IsEmpty() {
		panic("illegal operation")
	}
	return c.logIndex + 1
}

func (c *MimicRaftKernelCollector) LastIndex() uint64 {
	if c.IsNotInitialized() || c.IsEmpty() {
		panic("illegal operation")
	}
	return c.logIndex + uint64(len(c.content))
}

func (c *MimicRaftKernelCollector) IsEmpty() bool {
	return c.EntrySize() == 0
}

func (c *MimicRaftKernelCollector) EntrySize() int {
	if c.IsNotInitialized() {
		return 0
	}

	return len(c.content)
}

//GetLatestTerm gets the Term field of the last entry.
func (c *MimicRaftKernelCollector) GetLatestTerm() (bool, uint64) {
	if c.IsNotInitialized() || c.IsEmpty() {
		return false, 0
	}

	return true, c.content[len(c.content)-1].Term
}

func (c *MimicRaftKernelCollector) init(entries []raftpb.Entry, logTerm, logIndex uint64) {
	c.copied = false
	c.content = entries
	c.logTerm = logTerm
	c.logIndex = logIndex
	c.nextIndex = logIndex + uint64(len(entries)) + 1
	c.initialized = true
}

func (c *MimicRaftKernelCollector) directAddEntries(entries []raftpb.Entry) {
	if len(entries) == 0 {
		return
	}

	if !c.copied {
		c.content = append([]raftpb.Entry{}, c.content...)
		c.copied = true
	}

	c.content = append(c.content, entries...)
	c.nextIndex = entries[len(entries)-1].Index
}

func (c *MimicRaftKernelCollector) mimic(entries []raftpb.Entry, logTerm, logIndex uint64) (bool, Location) {
	// omit checking committed

	// check the existence of <logIndex, logTerm>
	loc := c.MatchIndex(logIndex, logTerm)
	if loc != PREV && loc != WITHIN {
		return false, loc
	}

	// find conflicts in entries
	_, cIdx := c.locateEntryWithIndex(logIndex + 1)
	cLen := len(c.content)
	eIdx, eLen := 0, len(entries)

	for cIdx < cLen && eIdx < eLen {
		if entries[eIdx].Term != c.content[cIdx].Term {
			break
		}
		cIdx++
		eIdx++
	}

	// append conflict entries
	if eIdx != eLen {
		c.destroyCachedTable()
		// extract the conflict part
		entries = entries[eIdx:]

		after := entries[0].Index
		if c.content[cLen-1].Index+1 == after {
			// direct append
			c.directAddEntries(entries)
		} else {
			// truncate, then append
			c.resize(cIdx, logTerm, logIndex)
			c.directAddEntries(entries)
		}
	}

	return true, loc
}

func (c *MimicRaftKernelCollector) resize(length int, logTerm, logIndex uint64) int {
	if length >= len(c.content) {
		return len(c.content)
	}

	if length == 0 {
		c.init(nil, logTerm, logIndex)
		return 0
	}

	if !c.copied {
		c.content = append([]raftpb.Entry{}, c.content[:length]...)
		c.copied = true
	} else {
		c.content = c.content[:length]
	}

	c.nextIndex = c.logIndex + uint64(length) + 1

	return length
}

func (c *MimicRaftKernelCollector) locateEntryWithIndex(index uint64) (bool, int) {
	rel := int(index - c.logIndex - 1)
	return rel >= 0 && rel < len(c.content), rel
}

func (c *MimicRaftKernelCollector) locateEntryWithTerm(term uint64, from, to int) (bool, int) {
	if ok, l, _ := c.locateCachedTableWithTerm(term, from, to); ok {
		return true, l
	}

	start := from
	end := to

	for start < end {
		mid := (start + end) / 2
		ct := c.content[mid].Term
		if ct == term {
			return true, mid
		} else if ct < term {
			start = mid + 1
		} else {
			end = mid
		}
	}

	return false, start
}

func (c *MimicRaftKernelCollector) locateFirstEntryWithTerm(term uint64, from, to int) (bool, int) {
	if ok, l, _ := c.locateCachedTableWithTerm(term, from, to); ok {
		return true, l
	}

	start := from
	end := to

	for start < end {
		mid := (start + end) / 2
		ct := c.content[mid].Term
		if ct == term {
			if mid == from || c.content[mid-1].Term != term {
				return true, mid
			}
			end = mid
		} else if ct < term {
			start = mid + 1
		} else {
			end = mid
		}
	}

	return false, start
}

func (c *MimicRaftKernelCollector) locateLastEntryWithTerm(term uint64, from, to int) (bool, int) {
	if ok, _, r := c.locateCachedTableWithTerm(term, from, to); ok {
		return true, r
	}

	start := from
	end := to

	for start < end {
		mid := (start + end) / 2
		ct := c.content[mid].Term
		if ct == term {
			if mid == to-1 || c.content[mid+1].Term != term {
				return true, mid
			}
			start = mid + 1
		} else if ct < term {
			start = mid + 1
		} else {
			end = mid
		}
	}

	return false, start
}

func (c *MimicRaftKernelCollector) destroyCachedTable() {
	if c.cachedTable == nil {
		return
	}
	c.cachedTable = c.cachedTable[:0]
}

func (c *MimicRaftKernelCollector) locateCachedTableWithTerm(term uint64, from, to int) (bool, int, int) {
	if len(c.cachedTable) == 0 {
		return false, 0, 0
	}

	start := 0
	end := len(c.cachedTable)

	for start < end {
		mid := (start + end) / 2
		mt := c.cachedTable[mid].term

		if mt == term {
			left := c.cachedTable[mid].first
			right := c.cachedTable[mid].last

			if left >= to || right < from {
				return false, 0, 0
			}

			if left < from {
				left = from
			}

			if right > to-1 {
				right = to - 1
			}

			return true, left, right
		} else if mt < term {
			start = mid + 1
		} else {
			end = mid
		}
	}

	return false, 0, 0
}
