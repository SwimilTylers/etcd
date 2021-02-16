package draft

import "go.etcd.io/etcd/raft/raftpb"

type Collector interface {
	AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool
	DropAllEntries() ([]raftpb.Entry, uint64, uint64)
	Refresh()
}

type LightWeightCollector struct {
	logTerm   uint64
	logIndex  uint64
	nextIndex uint64

	copied  bool
	content []raftpb.Entry
}

func (c *LightWeightCollector) AddEntries(entries []raftpb.Entry, logTerm uint64, logIndex uint64) bool {
	if len(entries) == 0 {
		return true
	}

	return c.addEntries(entries, logTerm, logIndex)
}

func (c *LightWeightCollector) DropAllEntries() ([]raftpb.Entry, uint64, uint64) {
	content := c.content
	c.content = nil
	c.copied = false
	return content, c.logTerm, c.logIndex
}

func (c *LightWeightCollector) Refresh() {
	if c.content == nil {
		return
	}

	c.DropAllEntries()
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

	var start = 0
	var end = cLen

	for start < end {
		mid := (start + end) / 2
		term := c.content[mid].Term

		if term == logTerm {
			// find the entry with logTerm
			index := c.content[mid].Index
			if index == logIndex {
				// hit the entry
				c.content = c.content[:mid+1]
				c.nextIndex = c.logIndex + uint64(mid) + 2
				return mid + 1
			} else if index > logIndex {
				mid -= int(index - logIndex)
				if mid < 0 {
					// entry is beyond reach, re-init the collector
					c.init(nil, logTerm, logIndex)
					return 0
				}

				if c.content[mid].Term == logTerm {
					// hit the entry
					c.content = c.content[:mid+1]
					c.nextIndex = c.logIndex + uint64(mid) + 2
					return mid + 1
				}

				panic("miss the entry, do not know how to handle it")

			} else {
				mid += int(logIndex - index)
				if mid >= cLen {
					// entry is beyond reach, ignore it
					return cLen
				}

				if c.content[mid].Term != logTerm {
					// find the latest entry with logTerm
					end = mid
					for start < end {
						mid = (start + end) / 2
						term = c.content[mid].Term

						if term == logTerm {
							if mid == cLen-1 || c.content[mid+1].Term > logTerm {
								break
							}

							start = mid + 1
						} else {
							end = mid
						}
					}
				}

				c.content = c.content[:mid+1]
				c.nextIndex = c.logIndex + uint64(mid) + 2
				return mid + 1
			}
		} else if term > logTerm {
			end = mid
		} else {
			start = mid + 1
		}
	}

	if start == 0 {
		c.init(nil, logTerm, logIndex)
		return 0
	}

	c.content = c.content[:start]
	c.nextIndex = c.logIndex + uint64(start) + 1
	return start
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

type CollectorFactory interface {
	GetCollector(rack, file string) Collector
}
