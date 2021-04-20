package collector

import (
	"go.etcd.io/etcd/raft/raftpb"
)

type EntryFetcher interface {
	//FetchEntriesWithStartIndex fetches entries with index >= startIndex, as well as their
	// previous LogTerm and LogIndex. Return false if no such an entry.
	FetchEntriesWithStartIndex(startIndex uint64) (bool, []raftpb.Entry, uint64, uint64)

	//FetchAllEntries fetches all entries from internal structure. Return false if it is empty.
	FetchAllEntries() (bool, []raftpb.Entry, uint64, uint64)
}

type Refresher interface {
	//IsNotInitialized checks if the collector is initialized.
	IsNotInitialized() bool

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

	EntryFetcher
	Briefer
	Refresher
}

type subCollector struct {
	Collector
	brief []*BriefSegment
	info  uint64
	prev  *subCollector
	next  *subCollector
}

//LinkedListCollector is an implementation of Collector. It allows
// the combination of several disjoint entry fragments (EntryFragment). Each fragments is a collection of
// entries with consecutive indices.
//
// The collector allows overlapping between fragments before internal regularization.
// The regularization will be automatically called if 1) it fetches some information (entries, fragments,
// brief segments, etc.) from the internal list, or 2) SetRegularized changes regularized from false to true.
// If you want to relieve the burden from regularization, make sure defaultRegOpt is true - the collector
// will not allow the temporary overlapping during AddEntries.
type LinkedListCollector struct {
	head *subCollector
	tail *subCollector

	regularized   bool
	defaultRegOpt bool

	cg func() Collector
}

func (c *LinkedListCollector) SubCECFactory() func() Collector {
	return c.cg
}

func (c *LinkedListCollector) SetSubCECFactory(subCECFactory func() Collector) {
	c.cg = subCECFactory
}

func NewLinkedListCollector(defaultRegOpt bool, cg func() Collector) *LinkedListCollector {
	return &LinkedListCollector{defaultRegOpt: defaultRegOpt, regularized: defaultRegOpt, cg: cg}
}

func (c *LinkedListCollector) AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	if c.regularized {
		c.regularizedAddEntries(entries, logTerm, logIndex)
	} else {
		c.addEntries(entries, logTerm, logIndex)
	}

	return true
}

func (c *LinkedListCollector) FetchEntries(term uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsNotInitialized() {
		return false, nil, 0, 0
	}

	c.regularize()

	if sub := c.locateEntries(term); sub != nil {
		return sub.FetchEntries(term)
	}

	return false, nil, 0, 0
}

func (c *LinkedListCollector) FetchEntriesWithStartIndex(index uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsNotInitialized() {
		return false, nil, 0, 0
	}

	c.regularize()

	_, e, t, i := c.head.FetchEntriesWithStartIndex(index)

	return c.head.next == nil, e, t, i
}

func (c *LinkedListCollector) FetchAllEntries() (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsNotInitialized() {
		return false, nil, 0, 0
	}

	c.regularize()

	_, e, t, i := c.head.FetchAllEntries()

	return c.head.next == nil, e, t, i
}

func (c *LinkedListCollector) Refresh() {
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

func (c *LinkedListCollector) Briefing() []*BriefSegment {
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

func (c *LinkedListCollector) IsNotInitialized() bool {
	return c.head == nil || c.head.IsNotInitialized()
}

func (c *LinkedListCollector) DefaultRegOpt() bool {
	return c.defaultRegOpt
}

func (c *LinkedListCollector) SetDefaultRegOpt(defaultRegOpt bool) {
	c.defaultRegOpt = defaultRegOpt
}

func (c *LinkedListCollector) Regularized() bool {
	return c.regularized
}

func (c *LinkedListCollector) SetRegularized(regularized bool) {
	if !c.regularized && regularized {
		c.regularize()
	}

	c.regularized = regularized
}

func (c *LinkedListCollector) briefing() []*BriefSegment {
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

func (c *LinkedListCollector) locateEntries(term uint64) Collector {
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

func (c *LinkedListCollector) regularizedAddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) {
	if c.head == nil {
		c.addSubAndMoveToTail().AddEntries(entries, logTerm, logIndex)
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

	c.addSubAndMoveToTail().AddEntries(entries, logTerm, logIndex)
}

func (c *LinkedListCollector) addEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) {
	if c.head == nil {
		c.addSubAndMoveToTail()
	}

	c.tail.brief = nil

	if ok := c.tail.AddEntries(entries, logTerm, logIndex); !ok {
		c.addSubAndMoveToTail().AddEntries(entries, logTerm, logIndex)
	}
}

//todo: verify and make it compatible to info fields
func (c *LinkedListCollector) regularize() {
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

			if needle.IsNotInitialized() {
				needle = c.tryRmvSubAndMoveToNext(needle)
				continue
			}

			next := needle.next

			for next != nil {
				ok, ent, lt, li := next.FetchAllEntries()

				if ok && !needle.AddEntries(ent, lt, li) {
					regular = false
					break
				}

				next = c.tryRmvSubAndMoveToNext(next)
			}

			needle = needle.next
		}

		if regular {
			break
		}
	}

	c.regularized = true
}

func (c *LinkedListCollector) addSubAndMoveToTail() *subCollector {
	if c.head == nil {
		c.head = &subCollector{Collector: c.cg()}
		c.tail = c.head
	} else {
		c.tail.next = &subCollector{Collector: c.cg(), prev: c.tail}
		c.tail = c.tail.next
	}

	return c.tail
}

func (c *LinkedListCollector) tryRmvSubAndMoveToNext(sc *subCollector) *subCollector {
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
