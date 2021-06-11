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
	Refresher
}

type nodeCollector struct {
	Collector

	startIndex uint64

	info uint64
	prev *nodeCollector
	next *nodeCollector
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
	head *nodeCollector
	tail *nodeCollector

	regularized   bool
	defaultRegOpt bool

	cg func() Collector
}

func (c *LinkedListCollector) NodeFactory() func() Collector {
	return c.cg
}

func (c *LinkedListCollector) SetNodeFactory(nodeFactory func() Collector) {
	c.cg = nodeFactory
}

func NewLinkedListCollector(defaultRegOpt bool, cg func() Collector) *LinkedListCollector {
	return &LinkedListCollector{defaultRegOpt: defaultRegOpt, regularized: defaultRegOpt, cg: cg}
}

func (c *LinkedListCollector) AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	if len(entries) == 0 {
		return false
	}

	c.addEntries(c.regularized, entries, logTerm, logIndex)

	return true
}

func (c *LinkedListCollector) FetchEntries(term uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsNotInitialized() {
		return false, nil, 0, 0
	}
	needle := c.head
	for needle != nil {
		if ok, ent, lt, li := needle.FetchEntries(term); ok {
			return true, ent, lt, li
		}
		needle = needle.next
	}

	return false, nil, 0, 0
}

func (c *LinkedListCollector) FetchEntriesWithStartIndex(index uint64) (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsNotInitialized() {
		return false, nil, 0, 0
	}

	_, e, t, i := c.head.FetchEntriesWithStartIndex(index)

	return c.head.next == nil, e, t, i
}

func (c *LinkedListCollector) FetchAllEntries() (bool, []raftpb.Entry, uint64, uint64) {
	if c.IsNotInitialized() {
		return false, nil, 0, 0
	}

	_, e, t, i := c.head.FetchAllEntries()

	return c.head.next == nil, e, t, i
}

func (c *LinkedListCollector) Refresh() {
	c.head = nil
	c.tail = nil
	c.regularized = c.defaultRegOpt
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
	c.regularized = regularized
}

func (c *LinkedListCollector) addEntries(regularize bool, entries []raftpb.Entry, logTerm uint64, logIndex uint64) *nodeCollector {
	if c.head == nil {
		c.head = c.newNodeCollector(entries, logTerm, logIndex)
		c.tail = c.head
		return c.head
	}

	needle := c.head

	for needle != nil {
		if needle.startIndex > logIndex+1 {
			// underflow occurs, reverse the order of the node
			node := c.newNodeCollector(entries, logTerm, logIndex)
			c.insertBefore(node, needle)

			if regularize {
				c.regularizeFrom(node)
			} else {
				c.deleteAfter(node)
			}

			return node
		}

		if ok := needle.AddEntries(entries, logTerm, logIndex); ok {
			c.deleteAfter(needle)
			return needle
		}

		needle = needle.next
	}

	node := c.newNodeCollector(entries, logTerm, logIndex)
	c.insertAfter(c.tail, node)
	return node
}

func (c *LinkedListCollector) newNodeCollector(entries []raftpb.Entry, logTerm, logIndex uint64) *nodeCollector {
	node := &nodeCollector{
		Collector:  c.cg(),
		startIndex: logIndex + 1,
	}

	node.AddEntries(entries, logTerm, logIndex)
	return node
}

func (c *LinkedListCollector) insertAfter(node *nodeCollector, after *nodeCollector) {
	node.prev = after
	node.next = after.next
	after.next = node

	if node.next == nil {
		c.tail = node
	} else {
		node.next.prev = node
	}
}

func (c *LinkedListCollector) insertBefore(node *nodeCollector, before *nodeCollector) {
	node.next = before
	node.prev = before.prev
	before.prev = node

	if node.prev == nil {
		c.head = node
	} else {
		node.prev.next = node
	}
}

func (c *LinkedListCollector) deleteAfter(node *nodeCollector) {
	if node.next != nil {
		node.next.prev = nil
	}
	node.next = nil
	c.tail = node
}

func (c *LinkedListCollector) deleteBefore(node *nodeCollector) {
	if node.prev != nil {
		node.prev.next = nil
	}
	node.prev = nil
	c.head = node
}

func (c *LinkedListCollector) regularizeFrom(node *nodeCollector) {
	needle := node.next
	for needle != nil {
		ok, ent, lt, li := needle.FetchAllEntries()
		if ok && node.AddEntries(ent, lt, li) {
			needle = needle.next
		} else {
			break
		}
	}
	c.deleteAfter(node)
}
