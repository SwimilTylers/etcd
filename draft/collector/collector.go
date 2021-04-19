package collector

import (
	"go.etcd.io/etcd/raft/raftpb"
)

type Refresher interface {
	//IsEmpty checks if the collector is empty.
	IsEmpty() bool

	//Refresh removes all internal status.
	Refresh()
}

//Collector works for Entry Appending and Conflict Resolution. It only deals with logTerm, logIndex and []Entry.
type Collector interface {
	//AddEntries collects entries and resolve potential conflict. This might cause
	// the modification of internal structure. Return false if failed to attach new entries.
	AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool

	//FetchEntries fetches all the entries with the request term, as well as their
	// previous LogTerm and LogIndex. Return false if no such an entry.
	FetchEntries(term uint64) (bool, []raftpb.Entry, uint64, uint64)

	//FetchEntriesWithStartIndex fetches entries with index >= startIndex, as well as their
	// previous LogTerm and LogIndex. Return false if no such an entry.
	FetchEntriesWithStartIndex(startIndex uint64) (bool, []raftpb.Entry, uint64, uint64)

	//FetchAllEntries fetches all entries from internal structure. Return false if it is empty.
	FetchAllEntries() (bool, []raftpb.Entry, uint64, uint64)

	Briefer
	Refresher
}

type CECLocation uint8

const (
	UNDERFLOW CECLocation = 0x0
	PREV      CECLocation = 0x1
	WITHIN    CECLocation = 0x2
	OVERFLOW  CECLocation = 0x4
	CONFLICT  CECLocation = 0x8
)

//ConsecutiveEntryCollector is a subtype of Collector. The entry it contains MUST be consecutively increasing.
type ConsecutiveEntryCollector interface {
	Collector

	//TryAddEntries works similar to Collector.AddEntries. If failed to add, it will return CECLocation for
	// further diagnosis:
	//	1. UNDERFLOW: logIndex is below the prevLogIndex of the entry array
	//	2. OVERFLOW: logIndex is above index of the last entry
	//	3. CONFLICT: find the conflict record in the entry array or in the tuple (prevLogIndex, prevLogTerm)
	TryAddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) (bool, CECLocation)

	//MatchIndex compares the given tuple to its own records.
	//	1. UNDERFLOW: index is below the prevLogIndex of the entry array
	//	2. PREV: (index, term) matches (prevLogIndex, prevLogTerm)
	//	3. WITHIN: find the same entry within the entry array
	//	4. OVERFLOW: index is beyond the maximum index recorded
	//	5. CONFLICT: find the conflict record in the entry array or in the tuple (prevLogIndex, prevLogTerm)
	MatchIndex(index, term uint64) CECLocation

	PrevLogTerm() uint64
	FirstIndex() uint64
	LastIndex() uint64

	//EntrySize returns the size of entry array
	EntrySize() int
	GetLatestTerm() (bool, uint64)
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

func NewSimplifiedRaftLogCollector() *MimicRaftKernelCollector {
	return &MimicRaftKernelCollector{copied: false}
}

func (c *MimicRaftKernelCollector) AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	ok, _ := c.TryAddEntries(entries, logTerm, logIndex)
	return ok
}

func (c *MimicRaftKernelCollector) FetchEntries(term uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsEmpty() {
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
	if c.IsEmpty() {
		return false, nil, 0, 0
	}

	if index <= c.logIndex+1 {
		return true, c.content, c.logTerm, c.logIndex
	}

	if index >= c.nextIndex {
		return false, nil, 0, 0
	}

	_, idx := c.locateEntryWithLogIndex(index - 1)
	before := &c.content[idx-1]
	return true, c.content[idx:], before.Term, before.Index
}

func (c *MimicRaftKernelCollector) FetchAllEntries() (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsEmpty() {
		return false, nil, 0, 0
	}

	return true, c.content, c.logTerm, c.logIndex
}

func (c *MimicRaftKernelCollector) Refresh() {
	if c.IsEmpty() {
		return
	}

	c.content = nil
	c.copied = false
	c.initialized = false
	c.destroyCachedTable()
}

func (c *MimicRaftKernelCollector) Briefing() []*BriefSegment {
	if c.IsEmpty() || c.content == nil {
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

func (c *MimicRaftKernelCollector) IsEmpty() bool {
	return !c.initialized
}

//TryAddEntries of MimicRaftKernelCollector mimics the behavior of raft kernel
func (c *MimicRaftKernelCollector) TryAddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) (bool, CECLocation) {
	if !c.IsEmpty() {
		c.init(entries, logTerm, logIndex)
		return true, PREV
	}

	/*
	* o.w. begin mimicking handleAppendEntries
	 */

	// omit checking committed

	// check the existence of <logIndex, logTerm>
	loc := c.MatchIndex(logIndex, logTerm)
	if loc != PREV && loc != WITHIN {
		return false, loc
	}

	// find conflicts in entries
	_, cIdx := c.locateEntryWithLogIndex(logIndex + 1)
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

func (c *MimicRaftKernelCollector) MatchIndex(index, term uint64) CECLocation {
	if c.IsEmpty() {
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
	} else if ok, idx := c.locateEntryWithLogIndex(index); !ok {
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

func (c *MimicRaftKernelCollector) PrevLogTerm() uint64 {
	if c.IsEmpty() {
		panic("not initialized")
	}
	return c.logTerm
}

func (c *MimicRaftKernelCollector) FirstIndex() uint64 {
	if c.IsEmpty() {
		panic("not initialized")
	}
	return c.content[0].Index
}

func (c *MimicRaftKernelCollector) LastIndex() uint64 {
	if c.IsEmpty() {
		panic("not initialized")
	}
	return c.logIndex + uint64(len(c.content))
}

//EntrySize returns the size of consecutive entry array
func (c *MimicRaftKernelCollector) EntrySize() int {
	if c.IsEmpty() {
		return 0
	}

	return len(c.content)
}

//GetLatestTerm gets the Term field of the last entry.
func (c *MimicRaftKernelCollector) GetLatestTerm() (bool, uint64) {
	if c.IsEmpty() {
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

func (c *MimicRaftKernelCollector) addEntries(entries []raftpb.Entry, logTerm, logIndex uint64) (bool, bool) {
	if c.content == nil || logIndex <= c.logIndex {
		c.init(entries, logTerm, logIndex)
		return true, false
	}

	if logIndex > c.nextIndex {
		return false, false
	}

	var oldLen = len(c.content)
	var truncated = false

	if !c.checkIfAppendable(logTerm, logIndex) {
		checkLen := c.rmvEntries(logTerm, logIndex)
		truncated = checkLen != oldLen
		if !c.checkIfAppendable(logTerm, logIndex) {
			return false, truncated
		}
		oldLen = checkLen
	}

	var newLen = len(entries)

	if oldLen == 0 {
		c.init(entries, logTerm, logIndex)
		return true, truncated
	}

	if newLen == 0 {
		return true, truncated
	}

	if !c.copied {
		content := make([]raftpb.Entry, oldLen, 2*(oldLen+newLen))
		copy(content, c.content[:oldLen])
		c.copied = true
		c.content = append(content, entries...)
	} else {
		c.content = c.content[:oldLen]
		c.content = append(c.content, entries...)
	}

	c.nextIndex = entries[newLen-1].Index + 1

	return true, truncated
}

func (c *MimicRaftKernelCollector) rmvEntries(logTerm, logIndex uint64) int {
	if len(c.content) == 0 {
		return 0
	}

	var cLen = len(c.content)
	if c.nextIndex-1 == logIndex && c.content[cLen-1].Term == logTerm {
		return cLen
	}

	legal, idx := c.locateEntryWithLogIndex(logIndex)

	if legal {
		if c.content[idx].Term == logTerm {
			return c.resize(idx+1, logTerm, logIndex)
		}

		term := c.content[idx].Term
		_, idx = c.locateFirstEntryWithTerm(term, 0, idx+1)

		if idx == 0 {
			return c.resize(0, logTerm, logIndex)
		}

		idx--
		term = c.content[idx].Term

		for term > logTerm {
			_, idx = c.locateFirstEntryWithTerm(term, 0, idx+1)
			if idx == 0 {
				break
			}
			idx--
			term = c.content[idx].Term
		}

		return c.resize(idx, logTerm, logIndex)
	}

	if idx < 0 {
		return c.resize(0, logTerm, logIndex)
	}

	return len(c.content)
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

func (c *MimicRaftKernelCollector) locateEntryWithLogIndex(logIndex uint64) (bool, int) {
	rel := int(logIndex - c.logIndex - 1)
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

func (c *MimicRaftKernelCollector) checkIfAppendable(logTerm, logIndex uint64) bool {
	if len(c.content) == 0 {
		return true
	}

	if c.nextIndex == logIndex+1 {
		return c.content[len(c.content)-1].Term == logTerm
	}

	return false
}

type subCollector struct {
	ConsecutiveEntryCollector
	brief     []*BriefSegment
	guarantor uint64
	prev      *subCollector
	next      *subCollector
}

//FragmentaryEntryCollector is an implementation of Collector. It allows
// the combination of several disjoint entry fragments (EntryFragment). Each fragments is a collection of
// entries with consecutive indices.
//
// The collector allows overlapping between fragments before internal regularization.
// The regularization will be automatically called if 1) it fetches some information (entries, fragments,
// brief segments, etc.) from the internal list, or 2) SetRegularized changes regularized from false to true.
// If you want to relieve the burden from regularization, make sure defaultRegOpt is true - the collector
// will not allow the temporary overlapping during AddEntries.
type FragmentaryEntryCollector struct {
	head *subCollector
	tail *subCollector

	regularized   bool
	defaultRegOpt bool
}

func NewFragmentaryEntryCollector(defaultRegOpt bool) *FragmentaryEntryCollector {
	return &FragmentaryEntryCollector{defaultRegOpt: defaultRegOpt, regularized: defaultRegOpt}
}

func (c *FragmentaryEntryCollector) AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	if c.regularized {
		c.regularizedAddEntries(entries, logTerm, logIndex)
	} else {
		c.addEntries(entries, logTerm, logIndex)
	}

	return true
}

func (c *FragmentaryEntryCollector) FetchEntries(term uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsEmpty() {
		return false, nil, 0, 0
	}

	c.regularize()

	if sub := c.locateEntries(term); sub != nil {
		return sub.FetchEntries(term)
	}

	return false, nil, 0, 0
}

func (c *FragmentaryEntryCollector) FetchEntriesWithStartIndex(index uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsEmpty() {
		return false, nil, 0, 0
	}

	c.regularize()

	_, e, t, i := c.head.FetchEntriesWithStartIndex(index)

	return c.head.next == nil, e, t, i
}

func (c *FragmentaryEntryCollector) FetchAllEntries() (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsEmpty() {
		return false, nil, 0, 0
	}

	c.regularize()

	_, e, t, i := c.head.FetchAllEntries()

	return c.head.next == nil, e, t, i
}

func (c *FragmentaryEntryCollector) Refresh() {
	if c.head == nil {
		return
	}
	c.head.brief = nil
	c.head.Refresh()
	if c.head.next != nil {
		c.head.next.prev = nil
		c.head.next = nil
	}
	c.regularized = c.defaultRegOpt
}

func (c *FragmentaryEntryCollector) Briefing() []*BriefSegment {
	if c.head == nil {
		return nil
	}

	if c.head.brief == nil {
		var ok bool
		c.head.brief = c.head.Briefing()
		if !ok {
			return nil
		}
	}

	return c.briefing()
}

func (c *FragmentaryEntryCollector) IsEmpty() bool {
	return c.head == nil || c.head.IsEmpty()
}

func (c *FragmentaryEntryCollector) DefaultRegOpt() bool {
	return c.defaultRegOpt
}

func (c *FragmentaryEntryCollector) SetDefaultRegOpt(defaultRegOpt bool) {
	c.defaultRegOpt = defaultRegOpt
}

func (c *FragmentaryEntryCollector) Regularized() bool {
	return c.regularized
}

func (c *FragmentaryEntryCollector) SetRegularized(regularized bool) {
	if !c.regularized && regularized {
		c.regularize()
	}

	c.regularized = regularized
}

func (c *FragmentaryEntryCollector) briefing() []*BriefSegment {
	var result []*BriefSegment

	needle := c.head
	for needle != nil {
		if needle.brief == nil {
			needle.brief = needle.Briefing()
		}
		result = append(result, needle.brief...)
		needle = needle.next
	}

	return result
}

func (c *FragmentaryEntryCollector) locateEntries(term uint64) Collector {
	needle := c.head
	for needle != nil {
		if needle.brief == nil {
			needle.brief = needle.Briefing()
		}

		b := needle.brief

		if b[0].Term > term {
			return nil
		} else if term <= b[len(b)-1].Term {
			var found = false
			for _, s := range b {
				if s.Term == term {
					found = true
					break
				}
			}

			if found {
				return needle
			}
		}

		needle = needle.next
	}

	return nil
}

func (c *FragmentaryEntryCollector) regularizedAddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) {
	if c.head == nil {
		c.addSubCollectorAndMoveToTail().AddEntries(entries, logTerm, logIndex)
		return
	}

	needle := c.head

	for needle != nil {
		if ok := needle.AddEntries(entries, logTerm, logIndex); ok {
			next := needle.next
			if next != nil {
				next.prev = nil
			}
			needle.next = nil
			return
		}
		needle = needle.next
	}

	c.addSubCollectorAndMoveToTail().AddEntries(entries, logTerm, logIndex)
}

func (c *FragmentaryEntryCollector) addEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) {
	if c.head == nil {
		c.addSubCollectorAndMoveToTail()
	}

	c.tail.brief = nil

	if ok := c.tail.AddEntries(entries, logTerm, logIndex); !ok {
		c.addSubCollectorAndMoveToTail().AddEntries(entries, logTerm, logIndex)
	}
}

//todo: verify and make it compatible to guarantor fields
func (c *FragmentaryEntryCollector) regularize() {
	if c.regularized {
		return
	}

	if c.head == nil || c.head.next == nil {
		c.regularized = true
		return
	}

	for {
		needle := c.head
		regular := true

		for needle != nil {
			if needle.brief == nil {
				break
			}
			// clear out buffered brief
			needle.brief = nil

			if needle.IsEmpty() {
				needle = c.tryRemoveSubCollectorAndMoveToNext(needle)
				continue
			}

			next := needle.next

			for next != nil {
				ok, ent, lt, li := next.FetchAllEntries()

				if ok && !needle.AddEntries(ent, lt, li) {
					regular = false
					break
				}

				next = c.tryRemoveSubCollectorAndMoveToNext(next)
			}

			needle = needle.next
		}

		if regular {
			break
		}
	}

	c.regularized = true
}

func (c *FragmentaryEntryCollector) addSubCollectorAndMoveToTail() *subCollector {
	if c.head == nil {
		c.head = &subCollector{ConsecutiveEntryCollector: &MimicRaftKernelCollector{}}
		c.tail = c.head
	} else {
		c.tail.next = &subCollector{ConsecutiveEntryCollector: &MimicRaftKernelCollector{}, prev: c.tail}
		c.tail = c.tail.next
	}

	return c.tail
}

func (c *FragmentaryEntryCollector) tryRemoveSubCollectorAndMoveToNext(sc *subCollector) *subCollector {
	if c.head == sc {
		if sc.next == nil {
			return nil
		}
		c.head = sc.next
		sc.next = nil
		c.head.prev = nil

		return c.head
	}

	if c.tail == sc {
		c.tail = sc.prev
		sc.prev = nil
		c.tail.next = nil

		return nil
	}

	next := sc.next
	sc.next = nil
	prev := sc.prev
	sc.prev = nil

	prev.next = next
	next.prev = prev

	return next
}
