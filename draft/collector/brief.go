package collector

import (
	"go.etcd.io/etcd/raft/raftpb"
)

type Briefer interface {
	//Briefing makes a brief report of the collector.
	Briefing() []*BriefSegment
}

type BriefSegment struct {
	Term        uint64
	PrevLogTerm uint64
	FirstIndex  uint64
	LastIndex   uint64
}

func (b *BriefSegment) Hit(term, index uint64) bool {
	return term == b.Term && b.FirstIndex <= index && index <= b.LastIndex
}

func (b *BriefSegment) HitPrev(term, index uint64) bool {
	return term == b.PrevLogTerm && b.FirstIndex-1 == index
}

func ExtractBriefFromEntries(prevLogTerm uint64, ent []raftpb.Entry) []*BriefSegment {
	var res []*BriefSegment
	var brief *BriefSegment = nil
	for _, entry := range ent {
		if brief != nil {
			if brief.Term == entry.Term {
				brief.LastIndex = entry.Index
				continue
			}

			res = append(res, brief)

			brief.PrevLogTerm = brief.Term
			brief.Term = entry.Term
			brief.FirstIndex = entry.Index
			brief.LastIndex = entry.Index
		} else {
			brief = &BriefSegment{entry.Term, prevLogTerm, entry.Index, entry.Index}
		}
	}

	if brief != nil {
		res = append(res, brief)
	}

	return res
}

type BriefSegmentCollector interface {
	//AddEntriesToBrief collects entries and resolve potential conflict. This might cause
	// the modification of internal structure. Return false if failed to attach new entries.
	AddEntriesToBrief(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool

	ResizeBriefToIndex(index uint64) (bool, Location)

	Locator
	Briefer
	Refresher
}

type MimicRaftKernelBriefCollector struct {
	b    []*BriefSegment
	next *BriefSegment

	logTerm  uint64
	logIndex uint64

	initialized bool
}

func NewMimicRaftKernelBriefCollector() *MimicRaftKernelBriefCollector {
	return &MimicRaftKernelBriefCollector{initialized: false}
}

func NewInitializedMimicRaftKernelBriefCollector(logTerm, logIndex uint64) *MimicRaftKernelBriefCollector {
	return &MimicRaftKernelBriefCollector{
		logTerm:     logTerm,
		logIndex:    logIndex,
		initialized: true,
	}
}

func CloneMimicRaftKernelBriefCollector(rkb *MimicRaftKernelBriefCollector) *MimicRaftKernelBriefCollector {
	if rkb == nil || !rkb.initialized {
		panic("illegal argument")
	}

	res := NewInitializedMimicRaftKernelBriefCollector(rkb.logTerm, rkb.logIndex)
	res.b = append([]*BriefSegment{}, rkb.b...)
	if len(res.b) != 0 {
		res.next = res.b[len(res.b)-1]
	}

	return res
}

func (c *MimicRaftKernelBriefCollector) AddEntriesToBrief(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	if c.IsNotInitialized() {
		c.init(entries, logTerm, logIndex)
		return true
	}

	ok, _ := c.mimic(entries, logTerm, logIndex)
	return ok
}

func (c *MimicRaftKernelBriefCollector) ResizeBriefToIndex(index uint64) (bool, Location) {
	if c.IsNotInitialized() || c.IsEmpty() {
		panic("illegal operation")
	}

	first, last := c.b[0].FirstIndex, c.b[len(c.b)-1].LastIndex

	switch {
	case index < first-1:
		return false, UNDERFLOW
	case index == first-1:
		c.b = nil
		c.next = nil
		return true, PREV
	case index > last:
		return false, OVERFLOW
	default:
		idx := c.locateIndex(index, 0, len(c.b))
		c.b = c.b[:idx+1]
		c.next = c.b[idx]
		c.next.LastIndex = index
		return true, WITHIN
	}
}

func (c *MimicRaftKernelBriefCollector) MatchIndex(index, term uint64) Location {
	if c.IsNotInitialized() {
		panic("not initialized")
	}
	l, _ := c.matchIndex(index, term)
	return l
}

func (c *MimicRaftKernelBriefCollector) LocateIndex(index uint64) (Location, uint64) {
	if c.IsNotInitialized() {
		panic("not initialized")
	}

	first, last := c.b[0].FirstIndex, c.b[len(c.b)-1].LastIndex

	switch {
	case index < first-1:
		return UNDERFLOW, -1
	case index == first-1:
		return PREV, c.b[0].PrevLogTerm
	case index > last:
		return OVERFLOW, -1
	default:
		idx := c.locateIndex(index, 0, len(c.b))
		return WITHIN, c.b[idx].Term
	}
}

func (c *MimicRaftKernelBriefCollector) PrevLogTerm() uint64 {
	if c.IsNotInitialized() {
		panic("not initialized")
	}
	return c.logTerm
}

func (c *MimicRaftKernelBriefCollector) FirstIndex() uint64 {
	if c.IsNotInitialized() || c.IsEmpty() {
		panic("illegal operation")
	}
	return c.logIndex + 1
}

func (c *MimicRaftKernelBriefCollector) LastIndex() uint64 {
	if c.IsNotInitialized() || c.IsEmpty() {
		panic("illegal operation")
	}
	return c.next.LastIndex
}

func (c *MimicRaftKernelBriefCollector) IsEmpty() bool {
	return c.next == nil
}

func (c *MimicRaftKernelBriefCollector) IsNotInitialized() bool {
	return !c.initialized
}

func (c *MimicRaftKernelBriefCollector) Refresh() {
	c.b = nil
	c.next = nil
	c.initialized = false
}

func (c *MimicRaftKernelBriefCollector) Briefing() []*BriefSegment {
	if c.IsNotInitialized() || c.IsEmpty() {
		return nil
	}

	return c.b
}

func (c *MimicRaftKernelBriefCollector) init(ent []raftpb.Entry, logTerm, logIndex uint64) {
	c.initialized = true
	c.logTerm = logTerm
	c.logIndex = logIndex

	c.b = ExtractBriefFromEntries(logTerm, ent)
	if len(c.b) != 0 {
		c.next = c.b[len(c.b)-1]
	} else {
		c.next = nil
	}
}

func (c *MimicRaftKernelBriefCollector) mimic(entries []raftpb.Entry, logTerm, logIndex uint64) (bool, Location) {
	// omit checking committed

	// check the existence of <logIndex, logTerm>
	loc := c.MatchIndex(logIndex, logTerm)
	if loc != PREV && loc != WITHIN {
		return false, loc
	}

	// find conflicts in entries
	eLen := len(entries)
	cLen := len(c.b)

	if eLen != 0 {
		if l, cIdx := c.matchIndex(entries[0].Index, entries[0].Term); l == WITHIN {
			brief := c.b[cIdx]
			eIdx := 0
			for eIdx < eLen && cIdx < cLen {
				ent := entries[eIdx]
				if !brief.Hit(ent.Term, ent.Index) {
					cIdx++
					if cIdx == cLen {
						break
					}
					brief = c.b[cIdx]
					if !brief.Hit(ent.Term, ent.Index) {
						break
					}
				}
				eIdx++
			}

			// append conflict entries
			if eIdx != eLen {
				entries = entries[eIdx:]
				var prevLogTerm uint64
				if eIdx == 0 {
					prevLogTerm = logTerm
				} else {
					prevLogTerm = entries[eIdx-1].Term
				}

				after := entries[0].Index
				if c.next.LastIndex+1 == after {
					// direct append
					c.absorbBriefs(ExtractBriefFromEntries(prevLogTerm, entries))
				} else {
					// truncate then append
					c.b = c.b[:cIdx+1]
					c.next = brief
					brief.LastIndex = after - 1
					c.absorbBriefs(ExtractBriefFromEntries(prevLogTerm, entries))
				}
			}
		}
	}

	return true, loc
}

func (c *MimicRaftKernelBriefCollector) locateTerm(term uint64, from, to int) int {
	start := from
	end := to

	for start < end {
		mid := (start + end) / 2
		t := c.b[mid].Term

		if t < term {
			start = mid + 1
		} else if term < t {
			end = mid
		} else {
			return mid
		}
	}

	return start
}

func (c *MimicRaftKernelBriefCollector) locateIndex(index uint64, from, to int) int {
	start := from
	end := to

	for start < end {
		mid := (start + end) / 2
		first, last := c.b[mid].FirstIndex, c.b[mid].LastIndex

		if last < index {
			start = mid + 1
		} else if index < first {
			end = mid
		} else {
			return mid
		}
	}

	return start
}

func (c *MimicRaftKernelBriefCollector) absorbBriefs(b []*BriefSegment) {
	if len(b) == 0 {
		return
	}

	if c.next == nil {
		c.b = b
		c.next = b[len(b)-1]

		return
	}

	succ := b[0]

	switch {
	case succ.Term == c.next.Term:
		c.next.LastIndex = succ.LastIndex
		c.b = append(c.b, b[1:]...)
		c.next = b[len(b)-1]
	case succ.PrevLogTerm == c.next.Term:
		c.b = append(c.b, b...)
		c.next = b[len(b)-1]
	}
}

func (c *MimicRaftKernelBriefCollector) matchIndex(index, term uint64) (Location, int) {
	first, last := c.b[0].FirstIndex, c.b[len(c.b)-1].LastIndex

	switch {
	case index < first-1:
		return UNDERFLOW, -1
	case index == first-1:
		if c.b[0].HitPrev(index, term) {
			return PREV, -1
		} else {
			return CONFLICT, -1
		}
	case index > last:
		return OVERFLOW, -1
	default:
		idx := c.locateTerm(term, 0, len(c.b))
		if c.b[idx].Hit(term, index) {
			return WITHIN, idx
		} else {
			return CONFLICT, -1
		}
	}
}
