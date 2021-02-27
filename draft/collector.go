package draft

import "go.etcd.io/etcd/raft/raftpb"

type CollectorBriefSegment struct {
	Term        uint64
	PrevLogTerm uint64
	FirstIndex  uint64
	LastIndex   uint64
}

//Collector works for Entry Appending and Conflict Resolution
type Collector interface {
	//AddEntries collects entries and resolve potential conflict.
	AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool

	FetchEntries(term uint64) (bool, []raftpb.Entry, uint64, uint64)

	//Refresh removes all internal status.
	Refresh()

	Briefing() (bool, []*CollectorBriefSegment)
}

//LightWeightCollector is an implementation of Collector. It can be viewed as an enhanced Entry array, which is
// designed for facilitating append-ops, while taking more time to resolve conflicts.
//
//This collector is recommended for collecting entries from a single source.
// As for this collector, DropAllEntries works the same as Refresh. Do mention this point.
type LightWeightCollector struct {
	logTerm   uint64
	logIndex  uint64
	nextIndex uint64

	copied  bool
	content []raftpb.Entry
}

func (c *LightWeightCollector) AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	return c.addEntries(entries, logTerm, logIndex)
}

func (c *LightWeightCollector) FetchEntries(term uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.content == nil {
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

func (c *LightWeightCollector) Refresh() {
	if c.content == nil {
		return
	}

	c.content = nil
	c.copied = false
}

func (c *LightWeightCollector) Briefing() (bool, []*CollectorBriefSegment) {
	if c.content == nil {
		return false, nil
	}

	var result []*CollectorBriefSegment
	left := 0
	logTerm := c.logTerm
	cLen := len(c.content)

	for left < cLen {
		term := c.content[left].Term
		_, right := c.locateLastEntryWithTerm(term, left, cLen)
		result = append(result, &CollectorBriefSegment{
			Term:        term,
			PrevLogTerm: logTerm,
			FirstIndex:  c.content[left].Index,
			LastIndex:   c.content[right].Index,
		})
		logTerm = term
		left = right + 1
	}

	return true, result
}

func (c *LightWeightCollector) init(entries []raftpb.Entry, logTerm, logIndex uint64) {
	c.copied = false
	c.content = entries
	c.logTerm = logTerm
	c.logIndex = logIndex
	c.nextIndex = logIndex + uint64(len(entries)) + 1
}

func (c *LightWeightCollector) addEntries(entries []raftpb.Entry, logTerm, logIndex uint64) bool {
	if c.content == nil || logIndex <= c.logIndex {
		c.init(entries, logTerm, logIndex)
		return true
	}

	if logIndex >= c.nextIndex {
		return false
	}

	var oldLen = len(c.content)

	if !c.checkIfAppendable(logTerm, logIndex) {
		oldLen = c.rmvEntries(logTerm, logIndex)
		if !c.checkIfAppendable(logTerm, logIndex) {
			return false
		}
	}

	var newLen = len(entries)

	if oldLen == 0 {
		c.init(entries, logTerm, logIndex)
		return true
	}

	if newLen == 0 {
		return true
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

	c.nextIndex = entries[newLen-1].Index

	return true
}

func (c *LightWeightCollector) rmvEntries(logTerm, logIndex uint64) int {
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
			return c.resize(idx, logTerm, logIndex)
		}

		_, idx = c.locateFirstEntryWithTerm(c.content[idx].Term, 0, idx+1)
		return c.resize(idx, logTerm, logIndex)
	}

	if idx < 0 {
		return c.resize(0, logTerm, logIndex)
	}

	return len(c.content)
}

func (c *LightWeightCollector) resize(length int, logTerm, logIndex uint64) int {
	if length >= len(c.content) {
		return len(c.content)
	}

	if length == 0 {
		c.init(nil, logTerm, logIndex)
		return 0
	}

	if !c.copied {
		content := make([]raftpb.Entry, length, 2*length)
		copy(content, c.content[:length])
		c.copied = true
	} else {
		c.content = c.content[:length]
	}

	c.nextIndex = c.logIndex + uint64(length) + 1

	return length
}

func (c *LightWeightCollector) locateEntryWithLogIndex(logIndex uint64) (bool, int) {
	rel := int(logIndex - c.logIndex - 1)
	return rel >= 0 && rel < len(c.content), rel
}

func (c *LightWeightCollector) locateEntryWithTerm(term uint64, from, to int) (bool, int) {
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

func (c *LightWeightCollector) locateFirstEntryWithTerm(term uint64, from, to int) (bool, int) {
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

func (c *LightWeightCollector) locateLastEntryWithTerm(term uint64, from, to int) (bool, int) {
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

func (c *LightWeightCollector) checkIfAppendable(logTerm, logIndex uint64) bool {
	if len(c.content) == 0 {
		return true
	}

	if c.nextIndex == logIndex+1 {
		return c.content[len(c.content)-1].Term == logTerm
	}

	return false
}

type subCollector struct {
	Collector

	brief []*CollectorBriefSegment

	next *subCollector
}

//HeavyWeightCollector is an implementation of Collector. It can be viewed as a linked list with some basic information,
// which is designed for resolving conflicts between several long entry arrays.
//
//This collector is recommended for combining entry arrays from different sources.
type HeavyWeightCollector struct {
	head *subCollector
	tail *subCollector
}

func (c *HeavyWeightCollector) AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	if c.head == nil {
		c.attachNewSubCollector()
	}

	c.tail.brief = nil

	if ok := c.tail.AddEntries(entries, logTerm, logIndex); !ok {
		c.attachNewSubCollector().AddEntries(entries, logTerm, logIndex)
	}

	return true
}

func (c *HeavyWeightCollector) FetchEntries(term uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.head == nil {
		return false, nil, 0, 0
	}

	if c.head.brief == nil {
		var ok bool
		ok, c.head.brief = c.head.Briefing()
		if !ok {
			return false, nil, 0, 0
		}
	}

	if sub := c.locateEntries(term); sub != nil {
		return sub.FetchEntries(term)
	}

	return false, nil, 0, 0
}

func (c *HeavyWeightCollector) Refresh() {
	if c.head == nil {
		return
	}
	c.head.brief = nil
	c.head.Refresh()
	if c.head.next != nil {
		c.head.next = nil
	}
}

func (c *HeavyWeightCollector) Briefing() (bool, []*CollectorBriefSegment) {
	if c.head == nil {
		return false, nil
	}

	if c.head.brief == nil {
		var ok bool
		ok, c.head.brief = c.head.Briefing()
		if !ok {
			return false, nil
		}
	}

	return true, c.briefing()
}

func (c *HeavyWeightCollector) briefing() []*CollectorBriefSegment {
	var result []*CollectorBriefSegment

	needle := c.head
	for needle != nil {
		if needle.brief == nil {
			_, needle.brief = needle.Briefing()
		}
		result = append(result, needle.brief...)
		needle = needle.next
	}

	return result
}

func (c *HeavyWeightCollector) locateEntries(term uint64) Collector {
	needle := c.head
	for needle != nil {
		if needle.brief == nil {
			_, needle.brief = needle.Briefing()
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

func (c *HeavyWeightCollector) attachNewSubCollector() Collector {
	if c.head == nil {
		c.head = &subCollector{Collector: &LightWeightCollector{}}
		c.tail = c.head
	} else {
		c.tail.next = &subCollector{Collector: &LightWeightCollector{}}
		c.tail = c.tail.next
	}

	return c.tail
}

type CollectorPool interface {
	RefreshAndGet(key string) Collector
	Get(key string) Collector
	Set(key string, c Collector)
}
