package collector

import (
	"fmt"
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

type BriefSegmentCollector struct {
	b      []*BriefSegment
	next   *BriefSegment
	commit uint64
}

func NewBriefSegmentCollector() *BriefSegmentCollector {
	return &BriefSegmentCollector{}
}

func (c *BriefSegmentCollector) IsInitialized() bool {
	return c.next == nil
}

func (c *BriefSegmentCollector) Refresh() {
	c.b = nil
	c.next = nil
	c.commit = 0
}

func (c *BriefSegmentCollector) Briefing() []*BriefSegment {
	return c.b
}

func (c *BriefSegmentCollector) PrevLogTerm() uint64 {
	return c.b[0].PrevLogTerm
}

func (c *BriefSegmentCollector) FirstIndex() uint64 {
	return c.b[0].FirstIndex
}

func (c *BriefSegmentCollector) LastIndex() uint64 {
	return c.next.LastIndex
}

func (c *BriefSegmentCollector) SetCommit(commit uint64) {
	c.commit = commit
}

func (c *BriefSegmentCollector) AbsorbEntries(logTerm, logIndex uint64, ent []raftpb.Entry) (bool, uint64) {
	l := len(c.b)
	if l == 0 {
		return false, 0
	}

	idx := c.locateTerm(logTerm, 0, l)
	if idx == l {
		return false, 0
	}

	brief := c.b[idx]
	if brief.Hit(logTerm, logIndex) || brief.HitPrev(logTerm, logIndex) {
		lastNewIndex := logIndex + uint64(len(ent))
		ci := c.findConflictIndex(ent, idx)
		switch {
		case ci == 0:
		case ci < c.commit:
			panic(fmt.Errorf("entry %d conflict with committed entry [committed(%d)]", ci, c.commit))
		default:
			offset := logIndex + 1
			if len(ent) > 0 {
				if ci == offset {
					c.absorbBriefs(ExtractBriefFromEntries(logTerm, ent))
				} else {
					c.absorbBriefs(ExtractBriefFromEntries(ent[ci-offset-1].Term, ent[ci-offset:]))
				}
			}
		}

		return true, lastNewIndex
	}

	return false, 0
}

func (c *BriefSegmentCollector) findConflictIndex(ent []raftpb.Entry, cStart int) uint64 {
	eIdx, eLen := 0, len(ent)
	cIdx, cLen := cStart, len(c.b)

	cc := c.b[cIdx]

	for eIdx < eLen && cIdx < cLen {
		e := ent[eIdx]

		if !cc.Hit(e.Term, e.Index) {
			cIdx++
			if cIdx == cLen {
				return e.Index
			}
			if cc = c.b[cIdx]; !cc.Hit(e.Term, e.Index) {
				return e.Index
			}
		}

		eIdx++
	}

	return 0
}

func (c *BriefSegmentCollector) locateTerm(term uint64, from, to int) int {
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

func (c *BriefSegmentCollector) absorbBriefs(b []*BriefSegment) {
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
